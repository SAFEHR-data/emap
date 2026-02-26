package uk.ac.ucl.rits.inform.datasources.waveform;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;
import uk.ac.ucl.rits.inform.datasources.waveform.hl7parse.Hl7ParseException;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

@Component
public class Hl7FromFile {
    private final Logger logger = LoggerFactory.getLogger(Hl7FromFile.class);

    private final Hl7ParseAndQueue hl7ParseAndQueue;
    private final ConfigurableApplicationContext applicationContext;
    private final String saveDirectory;

    static final String MESSAGE_DELIMITER = "\u001c";

    Hl7FromFile(Hl7ParseAndQueue hl7ParseAndQueue,
                ConfigurableApplicationContext applicationContext,
                @Value("${waveform.hl7.save.directory:#{null}}") String saveDirectory) {
        this.hl7ParseAndQueue = hl7ParseAndQueue;
        this.applicationContext = applicationContext;
        this.saveDirectory = saveDirectory;
    }


    /**
     * Entry point for ad-hoc replay from compressed HL7 files.
     * @return CommandLineRunner
     */
    @Bean
    @Profile("hl7-replay")
    public CommandLineRunner replayHl7FromBz2Files() {
        return (args) -> {
            Options options = new Options();
            options.addRequiredOption(null, "start-datetime", true, "Start datetime in UTC (e.g., 2023-01-01T00:00:00Z)");
            options.addRequiredOption(null, "end-datetime", true, "End datetime in UTC (e.g., 2023-01-01T23:59:59Z)");
            options.addOption(null, "source-location", true, "Location as found in HL7 waveform messages. Omit means all locations.");
            options.addOption(null, "dry-run", false, "Only print which files would be processed, do not actually process.");

            CommandLineParser parser = new DefaultParser();
            CommandLine cmd;
            try {
                cmd = parser.parse(options, args);
            } catch (ParseException e) {
                logger.error("Failed to parse command line arguments", e);
                throw new IllegalArgumentException("Invalid command line arguments", e);
            }

            Instant startDatetime = Instant.parse(cmd.getOptionValue("start-datetime"));
            Instant endDatetime = Instant.parse(cmd.getOptionValue("end-datetime"));
            String sourceLocation = cmd.getOptionValue("source-location");
            boolean dryRun = cmd.hasOption("dry-run");

            replaySpecifiedFiles(startDatetime, endDatetime, sourceLocation, dryRun);
            // Trigger Spring shutdown; QueueFlushLifecycle blocks until collator and publisher are drained
            applicationContext.close();
        };
    }

    private void replaySpecifiedFiles(Instant startDatetime, Instant endDatetime, String sourceLocation, boolean dryRun)
            throws Hl7ParseException, IOException, InterruptedException {
        logger.info("Replaying with startDatetime={}, endDatetime={}, sourceLocation={}",
                startDatetime, endDatetime, sourceLocation);

        List<File> filesToReplay = scanFiles(startDatetime, endDatetime, sourceLocation);

        try {
            for (File file : filesToReplay) {
                logger.info("Reading test HL7 file {}{}", (dryRun ? "[DRY RUN] " : ""), file);
                if (!dryRun) {
                    readAndQueueAllMessagesFromFile(file);
                    // Call collateAndSend at a predictable place (at the end of each file),
                    // rather than on a timer as we normally do when listening live.
                    hl7ParseAndQueue.collateAndSend();
                }
            }
        } catch (WaveformCollator.CollationException e) {
            throw new RuntimeException(e);
        }

        logger.info("All files read, finishing");
    }

    private boolean isMatch(Pattern pattern, Instant startDatetime, Instant endDatetime, Path path) {
        Matcher matcher = pattern.matcher(path.getFileName().toString());
        if (!matcher.matches()) {
            // would also happen if file name is otherwise malformed
            logger.debug("File name does not match expected pattern, likely location mismatch in path: {}", path);
            return false;
        }
        Instant fileTime = LocalDateTime.parse(matcher.group(1), Hl7MessageCompressor.FILE_NAME_DATETIME_PATTERN)
                .atOffset(ZoneOffset.UTC)
                .toInstant();
        // Half-open interval. Test purely on the time in the file name, which is the beginning of the period contained
        // in the file
        if (startDatetime.isAfter(fileTime)) {
            logger.debug("File datetime {} is earlier than start datetime {}, excluding", fileTime, startDatetime);
            return false;
        }
        if (!endDatetime.isAfter(fileTime)) {
            logger.debug("File datetime {} is equal or later than end datetime {}, excluding", fileTime, endDatetime);
            return false;
        }
        return true;
    }

    List<File> scanFiles(Instant startDatetime, Instant endDatetime, String sourceLocation) throws IOException {
        // This method is not ideal as it walks the entire directory tree and then discards files.
        // It would be better to limit our search to top-level dirs in between startDatetime and endDatetime.
        Path baseDir = Path.of(this.saveDirectory);
        // Don't check the directory names as all the info needed is in the file name
        String fileNameRegex = (sourceLocation != null ? sourceLocation : ".+") + "_(\\d{8}T\\d{4}Z)_[0-9a-f]+\\.hl7archive.bz2";
        Pattern fileNamePattern = Pattern.compile(fileNameRegex);
        List<File> files = new ArrayList<>();
        try (Stream<Path> paths = Files.walk(baseDir)) {
            paths
                    .filter(Files::isRegularFile)
                    .filter(p -> isMatch(fileNamePattern, startDatetime, endDatetime, p))
                    .sorted()
                    .map(Path::toFile)
                    .forEach(files::add);
        } catch (IOException e) {
            logger.warn("Error scanning directory {}: {}", baseDir, e.getMessage());
            throw e;
        }
        return files;
    }

    /**
     * Read messages from a single file with delimiter-separated messages.
     * This is the original format used for test dump files.
     * @param hl7InputStream InputStream containing delimiter-separated messages
     * @return List of message strings
     */
    List<String> readHl7MessagesFromInputStream(InputStream hl7InputStream) {
        try (Scanner scanner = new Scanner(hl7InputStream)) {
            scanner.useDelimiter(MESSAGE_DELIMITER);
            List<String> allMessages = new ArrayList<>();
            while (scanner.hasNext()) {
                String nextMessageStr = scanner.next();
                allMessages.add(nextMessageStr);
            }
            return allMessages;
        }
    }

    /**
     * Auto-detect compression status of optionally bz2 compressed HL7 file.
     *
     * @param hl7File A file containing hl7 messages delimited by an FS character.
     *                Must end in ".bz2" if and only if it is bz2 compressed,
     *                otherwise will be assumed to be uncompressed.
     * @return an InputStream from which uncompressed text can be read
     * @throws IOException if a bz2 file is corrupted
     */
    InputStream inputStreamFromFile(File hl7File) throws IOException {
        if (hl7File.toPath().toString().toLowerCase().endsWith(".bz2")) {
            logger.info("Detected bz2, reading compressed HL7 file {}", hl7File);
            InputStream fis = new FileInputStream(hl7File);
            try {
                return new BZip2CompressorInputStream(fis);
            } catch (IOException e) {
                fis.close();
                throw e;
            }
        } else {
            logger.info("Detected uncompressed, reading uncompressed HL7 file {}", hl7File);
            return new FileInputStream(hl7File);
        }
    }

    void readAndQueueAllMessagesFromFile(File hl7File) throws Hl7ParseException, WaveformCollator.CollationException, IOException {
        try (InputStream hl7InputStream = inputStreamFromFile(hl7File)) {
            readAndQueueAllMessagesFromStream(hl7InputStream);
        }
    }

    private void readAndQueueAllMessagesFromStream(InputStream hl7InputStream)
            throws Hl7ParseException, WaveformCollator.CollationException, IOException {
        List<String> messages = readHl7MessagesFromInputStream(hl7InputStream);
        logger.info("Read {} HL7 messages from test dump file", messages.size());
        for (int mi = 0; mi < messages.size(); mi++) {
            // do not re-save since we already took this from a file!
            hl7ParseAndQueue.saveParseQueue(messages.get(mi), false);
            if (mi % 100 == 0) {
                logger.info("handled {} messages out of {}", mi + 1, messages.size());
            }
        }
        logger.info("Queued {} HL7 messages from test dump file", messages.size());
    }
}
