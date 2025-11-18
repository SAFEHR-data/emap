package uk.ac.ucl.rits.inform.datasources.waveform;

import lombok.NonNull;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

/**
 * Compresses HL7 messages saved to disk into tar.bz2 archives.
 * Only processes hour directories that are fully in the past, to avoid
 * compressing something that's still being written to.
 * Compresses files on a per-bed-ID basis and deletes originals only after successful compression.
 * Runs as a scheduled task in a separate thread to avoid blocking other application tasks.
 */
@Component
public class Hl7MessageCompressor {
    private static final Logger logger = LoggerFactory.getLogger(Hl7MessageCompressor.class);

    private final Hl7MessageSaver messageSaver;
    private final boolean compressionEnabled;
    private final AtomicLong totalFilesCompressed = new AtomicLong(0);
    private final AtomicLong totalArchivesCreated = new AtomicLong(0);

    /**
     * Constructor for the HL7 message compressor.
     *
     * @param messageSaver The HL7 message saver that defines the save directory
     * @param compressionEnabled Whether compression is enabled (default: true)
     */
    @Autowired
    public Hl7MessageCompressor(
            @NonNull Hl7MessageSaver messageSaver,
            @Value("${waveform.hl7.compression.enabled:true}") boolean compressionEnabled) {
        this.messageSaver = messageSaver;
        this.compressionEnabled = compressionEnabled;

        if (!messageSaver.isSaveEnabled()) {
            logger.info("HL7 message compression DISABLED (saving is disabled)");
        } else if (!compressionEnabled) {
            logger.info("HL7 message compression DISABLED (explicitly disabled in config)");
        } else {
            logger.info("HL7 message compression ENABLED. Will compress complete directories");
        }
    }

    /**
     * Scheduled task that runs compression.
     * Can be overridden with waveform.hl7.compression.cron property.
     * @throws IOException if disk IO fails
     */
    @Scheduled(cron = "${waveform.hl7.compression.cron:0 5 * * * *}")
    public void compressOldMessagesScheduledTask() throws IOException {
        // In production, always call with real "now" time.
        // In test, scheduling will be disabled and you can pass anything in
        compressOldMessages(Instant.now());
    }

    void compressOldMessages(Instant nowTime) throws IOException {
        if (!messageSaver.isSaveEnabled() || !compressionEnabled) {
            return;
        }

        logger.info("Starting HL7 message compression task");
        long startTime = System.currentTimeMillis();
        int directoriesProcessed = 0;
        int archivesCreated = 0;

        Path saveDirectory = messageSaver.getSaveDirectory();

        // Find all hour directories that should be compressed
        List<Path> hourDirectories = findEligibleHourDirectories(saveDirectory, nowTime);

        for (Path hourDir : hourDirectories) {
            int archives = compressHourDirectory(hourDir);
            archivesCreated += archives;
            directoriesProcessed++;
        }

        long duration = System.currentTimeMillis() - startTime;
        logger.info("HL7 compression task completed in {}ms. Processed {} hour directories, created {} archives. "
                        + "Total stats: {} files compressed into {} archives",
                duration, directoriesProcessed, archivesCreated,
                totalFilesCompressed.get(), totalArchivesCreated.get());

    }

    /**
     * Find all hour directories that are eligible for compression.
     * A directory is eligible if it represents a complete hour that is fully in the past.
     *
     * @param saveDirectory The base save directory.
     * @param nowTime what time is now? (should =Instant.now() in production)
     * @return List of hour directory paths that should be compressed
     * @throws IOException If directory reading fails
     */
    private List<Path> findEligibleHourDirectories(Path saveDirectory, Instant nowTime) throws IOException {
        List<Path> eligibleDirs = new ArrayList<>();

        // Directories are named by their start time but they last 1 hour, so the cutoff
        // for start time is actually an hour before the nowTime
        Instant startCutoffTime = nowTime.minus(1, ChronoUnit.HOURS);
        DateTimeFormatter formatterAssumingUtc = Hl7MessageSaver.HOURLY_DIR_FORMATTER.withZone(ZoneId.of("UTC"));
        // List all directories in the save directory
        try (DirectoryStream<Path> timeDirs = Files.newDirectoryStream(saveDirectory, Files::isDirectory)) {
            for (Path hourDir : timeDirs) {
                String dirName = hourDir.getFileName().toString();

                try {
                    // Parse the directory name as an Instant representing the UTC hour
                    Instant dirInstant = Instant.from(formatterAssumingUtc.parse(dirName));

                    // Check if this directory is old enough to compress
                    if (dirInstant.isBefore(startCutoffTime)) {
                        // Check if there are any uncompressed files in this directory
                        if (hasUncompressedFiles(hourDir)) {
                            eligibleDirs.add(hourDir);
                        }
                    } else {
                        logger.debug("Skipping directory {}, too new", dirName);
                    }
                } catch (DateTimeParseException e) {
                    // Not a valid hour directory, skip it
                    logger.debug("Skipping non-hour directory: {}", dirName);
                }
            }
        }

        logger.debug("Found {} hour directories eligible for compression", eligibleDirs.size());
        return eligibleDirs;
    }

    /**
     * Check if a directory (or its subdirectories) contains any uncompressed .hl7 files.
     *
     * @param directory The directory to check
     * @return true if uncompressed files exist, false otherwise
     * @throws IOException If directory reading fails
     */
    private boolean hasUncompressedFiles(Path directory) throws IOException {
        try (Stream<Path> walk = Files.walk(directory)) {
            return walk.anyMatch(path -> path.toString().endsWith(".hl7"));
        }
    }

    /**
     * Compress all files in an hour directory, organized by bed ID.
     * Creates one tar.bz2 file per bed ID subdirectory.
     *
     * @param hourDir The hour directory to compress
     * @return Number of archives created
     * @throws IOException If compression fails
     */
    private int compressHourDirectory(Path hourDir) throws IOException {
        int archivesCreated = 0;

        // List all bed ID subdirectories
        try (DirectoryStream<Path> bedDirs = Files.newDirectoryStream(hourDir, Files::isDirectory)) {
            for (Path bedDir : bedDirs) {
                String bedId = bedDir.getFileName().toString();

                // Check if there are any .hl7 files to compress
                List<Path> hl7Files = findHl7Files(bedDir);
                if (hl7Files.isEmpty()) {
                    logger.debug("No HL7 files to compress in {}/{}", hourDir.getFileName(), bedId);
                    continue;
                }

                // Create archive name: {hourDir}/{bedId}.tar.bz2
                String archiveName = bedId + ".tar.bz2";
                Path archivePath = bedDir.resolveSibling(archiveName);

                // If archive already exists but we have managed to get to this point,
                // assume that hl7 deletion got interrupted. Because HL7
                // messages may be partially deleted, attempting to recreate the archive could
                // result in data loss, so leave it intact and progress on to re-attempt the deletion.
                if (Files.exists(archivePath)) {
                    logger.warn("Archive already exists, not recreating: {}", archivePath);
                }

                Path tempArchive = bedDir.resolveSibling(archiveName + ".tmp");
                try {
                    // Create temporary archive first so we can always detect a partially created archive
                    createTarBz2Archive(tempArchive, hl7Files, bedDir);

                    // Atomically rename temp file to final name
                    Files.move(tempArchive, archivePath);

                    // Only delete originals if archive was created successfully
                    deleteFiles(hl7Files);

                    // Try to delete the now-empty bed directory
                    deleteEmptyDirectory(bedDir);

                    archivesCreated++;
                    totalArchivesCreated.incrementAndGet();
                    totalFilesCompressed.addAndGet(hl7Files.size());

                    logger.info("Created archive {} with {} files", archivePath, hl7Files.size());

                } catch (IOException e) {
                    logger.error("Failed to create archive for {}/{}: {}",
                            hourDir.getFileName(), bedId, e.getMessage(), e);
                    // To help in the case where this is caused by out of disk space,
                    // delete temporary file if it exists, but you can't assume this
                    // will run successfully
                    if (Files.exists(tempArchive)) {
                        try {
                            Files.delete(tempArchive);
                        } catch (IOException deleteEx) {
                            logger.warn("Also failed to delete temporary archive: {}", tempArchive, deleteEx);
                            throw deleteEx;
                        }
                    }
                    throw e;
                }
            }
        }

        // Try to delete the hour directory if it's now empty
        deleteEmptyDirectory(hourDir);

        return archivesCreated;
    }

    /**
     * Find all .hl7 files in a directory (non-recursive).
     *
     * @param directory The directory to search
     * @return List of .hl7 file paths
     * @throws IOException If directory reading fails
     */
    private List<Path> findHl7Files(Path directory) throws IOException {
        List<Path> hl7Files = new ArrayList<>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(directory, "*.hl7")) {
            for (Path file : stream) {
                if (Files.isRegularFile(file)) {
                    hl7Files.add(file);
                }
            }
        }
        return hl7Files;
    }

    /**
     * Create a tar.bz2 archive containing the specified files.
     * File paths in the archive are relative to the baseDir (just the filename).
     *
     * @param archivePath Path where the archive should be created
     * @param filesToArchive       List of files to include in the archive
     * @param baseDir     Base directory for calculating relative paths
     * @throws IOException If archive creation fails
     */
    private void createTarBz2Archive(Path archivePath, List<Path> filesToArchive, Path baseDir) throws IOException {
        try (BufferedOutputStream buffOut = new BufferedOutputStream(Files.newOutputStream(archivePath));
             BZip2CompressorOutputStream bzOut = new BZip2CompressorOutputStream(buffOut);
             TarArchiveOutputStream tarOut = new TarArchiveOutputStream(bzOut)) {

            // Set to handle long file names
            tarOut.setLongFileMode(TarArchiveOutputStream.LONGFILE_POSIX);
            tarOut.setBigNumberMode(TarArchiveOutputStream.BIGNUMBER_POSIX);

            for (Path file : filesToArchive) {
                // Use the relative path in the archive because:
                // - paths will look different inside vs outside the container so abs path doesn't make sense
                // - including the bed ID allows the original file structure to be recreated (not that we intend to do so)
                // - it's NOT about uniqueness, because the random string in the hl7 filenames already provides that
//                Path relPath = file.relativize(baseDir);
//                String tarEntryName = relPath.toString();

                TarArchiveEntry entry = new TarArchiveEntry(file.toFile());
                tarOut.putArchiveEntry(entry);

                // Write file contents
                Files.copy(file, tarOut);

                tarOut.closeArchiveEntry();
            }

            tarOut.finish();
        }
    }

    /**
     * Delete a list of files.
     *
     * @param files Files to delete
     */
    private void deleteFiles(List<Path> files) {
        for (Path file : files) {
            try {
                Files.delete(file);
            } catch (IOException e) {
                logger.error("Failed to delete file after compression: {}", file, e);
            }
        }
    }

    /**
     * Try to delete a directory if it's empty.
     *
     * @param directory Directory to delete
     */
    private void deleteEmptyDirectory(Path directory) {
        try {
            // This will only succeed if the directory is empty
            Files.delete(directory);
            logger.debug("Deleted empty directory: {}", directory);
        } catch (IOException e) {
            // Directory is not empty or couldn't be deleted - that's fine
            logger.trace("Could not delete directory (may not be empty): {}", directory);
        }
    }

    /**
     * Get statistics on compression activity.
     *
     * @return Total number of files compressed
     */
    public long getTotalFilesCompressed() {
        return totalFilesCompressed.get();
    }

    /**
     * Get statistics on compression activity.
     *
     * @return Total number of archives created
     */
    public long getTotalArchivesCreated() {
        return totalArchivesCreated.get();
    }
}

