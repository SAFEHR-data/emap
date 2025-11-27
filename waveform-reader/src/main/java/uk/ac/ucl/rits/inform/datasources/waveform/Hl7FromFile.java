package uk.ac.ucl.rits.inform.datasources.waveform;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import uk.ac.ucl.rits.inform.datasources.waveform.hl7parse.Hl7ParseException;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Component
public class Hl7FromFile {
    private final Logger logger = LoggerFactory.getLogger(Hl7FromFile.class);

    private final Hl7ParseAndQueue hl7ParseAndQueue;
    private final File hl7DumpFile;
    static final String MESSAGE_DELIMITER = "\u001c";

    Hl7FromFile(Hl7ParseAndQueue hl7ParseAndQueue,
                @Value("${waveform.hl7.test_dump_file:#{null}}") File hl7DumpFile
    ) {
        this.hl7ParseAndQueue = hl7ParseAndQueue;
        this.hl7DumpFile = hl7DumpFile;
    }

    /**
     * Read messages from a single file with delimiter-separated messages.
     * This is the original format used for test dump files.
     * @param hl7DumpFile File containing delimiter-separated messages
     * @return List of message strings
     * @throws IOException if file cannot be read
     */
    List<String> readFromFile(File hl7DumpFile) throws IOException {
        logger.info("Reading test HL7 file {}", hl7DumpFile);
        Scanner scanner = new Scanner(hl7DumpFile);
        scanner.useDelimiter(MESSAGE_DELIMITER);
        List<String> allMessages = new ArrayList<>();
        while (scanner.hasNext()) {
            String nextMessageStr = scanner.next();
            allMessages.add(nextMessageStr);
        }
        return allMessages;
    }

    /**
     * Read messages from a directory tree organized by date and hour.
     * Supports the new structure: {base_dir}/{YYYY-MM-DD}/{HH}/*.hl7
     * Files are processed in chronological order based on their path and name.
     * @param directory Root directory containing date subdirectories
     * @return List of message strings
     * @throws IOException if directory cannot be read
     */
    List<String> readFromDirectory(File directory) throws IOException {
        logger.info("Reading HL7 messages from directory tree {}", directory);
        Path basePath = directory.toPath();

        if (!Files.isDirectory(basePath)) {
            throw new IOException("Not a directory: " + directory);
        }

        List<String> allMessages = new ArrayList<>();

        // Walk the directory tree and collect all .hl7 files
        try (Stream<Path> paths = Files.walk(basePath)) {
            List<Path> hl7Files = paths
                    .filter(Files::isRegularFile)
                    .filter(p -> p.toString().endsWith(".hl7"))
                    .sorted() // Sort by path (naturally sorts by date/hour/timestamp)
                    .collect(Collectors.toList());

            logger.info("Found {} HL7 files to process", hl7Files.size());

            for (Path filePath : hl7Files) {
                try {
                    String content = Files.readString(filePath);
                    if (content != null && !content.trim().isEmpty()) {
                        allMessages.add(content);
                    }
                } catch (IOException e) {
                    logger.error("Failed to read file {}, skipping", filePath, e);
                }
            }
        }

        logger.info("Read {} messages from directory tree", allMessages.size());
        return allMessages;
    }

    @Scheduled(fixedRate = Long.MAX_VALUE) // do once only
    void readOnceAndQueueScheduled() throws Hl7ParseException, WaveformCollator.CollationException, IOException {
        if (hl7DumpFile == null) {
            logger.info("No test HL7 file specified");
            return;
        }
        readOnceAndQueue(hl7DumpFile);
        // Not sure how to wait for Publisher to finish, so just sleep for a bit
        try {
            Thread.sleep(10_000);
        } catch (InterruptedException e) {
            logger.warn("Thread was interrupted", e);
        }
        System.exit(0);
    }

    /**
     * Read and queue messages from either a single file or a directory tree.
     * Automatically detects whether the path is a file or directory.
     * @param hl7Source File or directory containing HL7 messages
     * @throws Hl7ParseException if HL7 parsing fails
     * @throws WaveformCollator.CollationException if collation fails
     * @throws IOException if file/directory reading fails
     */
    void readOnceAndQueue(File hl7Source) throws Hl7ParseException, WaveformCollator.CollationException, IOException {
        List<String> messages;

        if (hl7Source.isDirectory()) {
            // New format: directory tree with hourly subdirectories
            messages = readFromDirectory(hl7Source);
        } else {
            // Old format: single file with delimiter-separated messages
            messages = readFromFile(hl7Source);
        }

        logger.info("Read {} HL7 messages, beginning to queue", messages.size());
        for (int mi = 0; mi < messages.size(); mi++) {
            // do not re-save since we already took this from a file!
            hl7ParseAndQueue.saveParseQueue(messages.get(mi), false);
            if (mi % 100 == 0) {
                logger.info("handled {} messages out of {}", mi + 1, messages.size());
            }
        }
        logger.info("Queued {} HL7 messages", messages.size());
    }
}
