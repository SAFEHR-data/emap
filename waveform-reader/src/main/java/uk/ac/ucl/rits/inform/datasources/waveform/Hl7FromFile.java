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
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

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
            // Use Commons CLI (standard and robust) to parse command line arguments
            Options options = new Options();
            options.addRequiredOption(null, "start-datetime", true, "Start datetime in UTC (e.g., 2023-01-01T00:00:00Z)");
            options.addRequiredOption(null, "end-datetime", true, "End datetime in UTC (e.g., 2023-01-01T23:59:59Z)");
            options.addRequiredOption(null, "source-location", true, "Location as found in HL7 waveform messages");

            CommandLineParser parser = new DefaultParser();
            CommandLine cmd;
            try {
                cmd = parser.parse(options, args);
            } catch (ParseException e) {
                logger.error("Failed to parse command line arguments", e);
                throw new IllegalArgumentException("Invalid command line arguments", e);
            }

            String startDatetime = cmd.getOptionValue("start-datetime");
            String endDatetime = cmd.getOptionValue("end-datetime");
            String sourceLocation = cmd.getOptionValue("source-location");

            logger.info("Replaying with startDatetime={}, endDatetime={}, sourceLocation={}",
                    startDatetime, endDatetime, sourceLocation);

            List<File> filesToReplay = scanFiles(startDatetime, endDatetime, sourceLocation);

            try {
                for (File file : filesToReplay) {
                    logger.info("Reading test HL7 file {}", file);
                    readAndQueueAllMessagesFromFile(file);
                    // Call collateAndSend at a predictable place (at the end of each file),
                    // rather than on a timer as we normally do when listening live.
                    hl7ParseAndQueue.collateAndSend();
                }
            } catch (WaveformCollator.CollationException e) {
                throw new RuntimeException(e);
            }

            // Trigger Spring shutdown; QueueFlushLifecycle blocks until collator and publisher are drained
            logger.info("All files read, initiating shutdown (queues will be flushed)");
            applicationContext.close();
        };
    }

    private List<File> scanFiles(String startDatetime, String endDatetime, String sourceLocation) {
        // XXX: stub implementation that only returns one file
        Path baseDir = Path.of(this.saveDirectory);
        return List.of(
                baseDir.resolve("20240829T00/UCHT03ICUBED26/UCHT03ICUBED26_20240829T0000Z_24aa4c1196f938e8.hl7archive.bz2").toFile()
        );
    }

    /**
     * Read messages from a single file with delimiter-separated messages.
     * This is the original format used for test dump files.
     * @param hl7InputStream InputStream containing delimiter-separated messages
     * @return List of message strings
     * @throws IOException if file cannot be read
     */
    List<String> readHl7MessagesFromInputStream(InputStream hl7InputStream) throws IOException {
        Scanner scanner = new Scanner(hl7InputStream);
        scanner.useDelimiter(MESSAGE_DELIMITER);
        List<String> allMessages = new ArrayList<>();
        while (scanner.hasNext()) {
            String nextMessageStr = scanner.next();
            allMessages.add(nextMessageStr);
        }
        return allMessages;
    }

    InputStream inputStreamFromBz2File(File bz2File) throws IOException {
        FileInputStream fis = null;
        try {
            fis = new FileInputStream(bz2File);
            return new BZip2CompressorInputStream(fis);
        } catch (IOException e) {
            if (fis != null) {
                fis.close();
            }
            throw e;
        }
    }

    void readAndQueueAllMessagesFromFile(File hl7File) throws Hl7ParseException, WaveformCollator.CollationException, IOException {
        InputStream hl7InputStream = null;
        if (hl7File.toPath().toString().toLowerCase().endsWith(".bz2")) {
            logger.info("Detected bz2, reading compressed HL7 file {}", hl7File);
            hl7InputStream = inputStreamFromBz2File(hl7File);
        } else {
            logger.info("Detected uncompressed, reading uncompressed HL7 file {}", hl7File);
            hl7InputStream = new FileInputStream(hl7File);
        }
        readAndQueueAllMessagesFromStream(hl7InputStream);
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
