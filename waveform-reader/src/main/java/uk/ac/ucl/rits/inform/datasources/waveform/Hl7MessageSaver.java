package uk.ac.ucl.rits.inform.datasources.waveform;

import lombok.Getter;
import lombok.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
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
    private final Hl7MessageCompressor hl7MessageCompressor;

    /**
     * Constructor that initializes the message saver.
     * @param saveDirectoryPath Path to the directory where messages should be saved.
     *                          If null or empty, saving is disabled.
     * @param hl7MessageCompressor message compressor
     * @throws IOException If cannot create directory.
     * @throws IllegalArgumentException if base directory does not exist
     */
    public Hl7MessageSaver(@Value("${waveform.hl7.save_directory:#{null}}") String saveDirectoryPath,
                           @Autowired Hl7MessageCompressor hl7MessageCompressor) throws IOException {
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
        this.hl7MessageCompressor = hl7MessageCompressor;
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
     * @param bedId bed (location) Id
     * @throws IOException If writing failed
     */
    public void saveMessage(@NonNull String messageContent, @NonNull Instant messageTimestamp, @NonNull String bedId) throws IOException {
        if (saveEnabled) {
            hl7MessageCompressor.saveMessage(messageContent, bedId, messageTimestamp);
        }

        long count = messageCounter.incrementAndGet();
        if (count % 100000 == 1) {
            String verb = saveEnabled ? "SAVED" : "Saving DISABLED: would have saved";
            logger.info("{} {} HL7 message(s) to disk so far", verb, count);
        }
    }

    /**
     * Get the total number of messages saved so far.
     * @return count of saved messages
     */
    public long getSavedMessageCount() {
        return messageCounter.get();
    }
}

