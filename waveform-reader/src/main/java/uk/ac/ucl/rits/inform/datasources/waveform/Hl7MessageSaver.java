package uk.ac.ucl.rits.inform.datasources.waveform;

import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Saves incoming HL7 messages to disk in hourly subdirectories.
 * Saving is optional and controlled by configuration.
 */
@Component
public class Hl7MessageSaver {
    private final Logger logger = LoggerFactory.getLogger(Hl7MessageSaver.class);

    @Getter
    private final Path saveDirectory;
    @Getter
    private final boolean saveEnabled;
    private final AtomicLong messageCounter = new AtomicLong(0);

    /**
     * Constructor that initializes the message saver.
     * @param saveDirectoryPath Path to the directory where messages should be saved.
     *                          If null or empty, saving is disabled.
     * @throws IOException If cannot create directory.
     * @throws IllegalArgumentException if base directory does not exist
     */
    public Hl7MessageSaver(@Value("${waveform.hl7.save_directory:#{null}}") String saveDirectoryPath) throws IOException {
        if (saveDirectoryPath == null || saveDirectoryPath.trim().isEmpty()) {
            this.saveEnabled = false;
            this.saveDirectory = null;
            logger.info("HL7 message saving DISABLED");
        } else {
            this.saveEnabled = true;
            this.saveDirectory = Paths.get(saveDirectoryPath);

            /* Don't create the base directory as it will likely need to be manually
             * set up with particular permissions etc.
             */
            if (!Files.isDirectory(this.saveDirectory)) {
                throw new IllegalArgumentException("HL7 message saving in directory '" + this.saveDirectory + "' is not a directory");
            }
            logger.info("HL7 message saving in directory: {}", this.saveDirectory);
        }
    }

    /**
     * Save an HL7 message to disk.
     * Messages are organized into hourly subdirectories with the structure:
     * {base_directory}/{YYYY-MM-DD}/{HH}/{bedid}/{timestamp}.hl7
     * Parsing of timestamp and bed ID should have already happened, so
     * no need to parse it out here.
     *
     * @param messageContent The raw HL7 message string to save
     * @param messageTimestamp timestamp at the message (not observation) level
     * @param bedId bed (lcoation) Id
     * @throws IOException If writing failed
     */
    public void saveMessage(String messageContent, Instant messageTimestamp, String bedId) throws IOException {
        Path targetPath = buildTargetPath(bedId, messageTimestamp);

        // Ensure the hourly directory exists
        Files.createDirectories(targetPath.getParent());

        // Write the message to file
        try (FileWriter writer = new FileWriter(targetPath.toFile())) {
            writer.write(messageContent);
        }

        long count = messageCounter.incrementAndGet();
        if (count % 1000 == 0) {
            logger.info("Saved {} HL7 messages to disk so far", count);
        } else {
            logger.debug("Saved HL7 message to {}", targetPath);
        }

    }

    /**
     * Build the target file path for a message.
     * // XXX: wrong
     * Format: {base_save_directory}/{YYYY-MM-DD}/{HH}/{timestamp}_{uuid}_{counter}.hl7
     *
     * @param bedId bed ID
     * @param timestamp The timestamp for the message
     * @return Path object for the target file
     */
    private Path buildTargetPath(String bedId, Instant timestamp) {
        String dateDir = DateTimeFormatter.ofPattern("yyyyMMdd'T'HH")
                .withZone(ZoneOffset.UTC)
                .format(timestamp);

        // eg. "14"
        String hourDir = DateTimeFormatter.ofPattern("HH")
                .withZone(ZoneOffset.UTC)
                .format(timestamp);

        // Eg. "20251030T142345.123Z"
        String fileName = DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss.SSS'Z'")
                .withZone(ZoneOffset.UTC)
                .format(timestamp);

        return saveDirectory.resolve(dateDir).resolve(bedId).resolve(fileName);
    }

    /**
     * Get the total number of messages saved so far.
     * @return count of saved messages
     */
    public long getSavedMessageCount() {
        return messageCounter.get();
    }
}

