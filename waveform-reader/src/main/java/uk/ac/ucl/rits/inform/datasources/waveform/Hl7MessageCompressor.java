package uk.ac.ucl.rits.inform.datasources.waveform;

import lombok.Getter;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Handle the per-bed, per-time period, bz2 archives.
 */
@Component
public class Hl7MessageCompressor {
    private static final Logger logger = LoggerFactory.getLogger(Hl7MessageCompressor.class);
    static final DateTimeFormatter HOURLY_DIR_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd'T'HH");

    @Getter
    private final Path saveDirectory;

    private final AtomicLong totalFilesCompressed = new AtomicLong(0);
    private final AtomicLong totalArchivesCreated = new AtomicLong(0);

    // bedId + timeslot -> output stream
    private final Map<ImmutablePair<String, Instant>, BZip2CompressorOutputStream> openArchives
            = new HashMap<>();
    // bedId + timeslot -> last written to
    private final Map<ImmutablePair<String, Instant>, Instant> openArchivesLastWritten
            = new HashMap<>();
    private final ThreadPoolTaskExecutor closeFileExecutor;
    private volatile boolean isShuttingDown = false;

    /**
     * Constructor for the HL7 message compressor.
     * @param saveDirectory base directory to save compressed archives in
     * @param closeFileThreadPoolExecutor single-thread executor for closing files
     */
    @Autowired
    public Hl7MessageCompressor(
            @Value("${waveform.hl7.save_directory:#{null}}") String saveDirectory,
            ThreadPoolTaskExecutor closeFileThreadPoolExecutor) {
        this.saveDirectory = Path.of(saveDirectory);
        this.closeFileExecutor = closeFileThreadPoolExecutor;
    }

    private BZip2CompressorOutputStream getOutputStream(String bedId, Instant roundedTime) throws IOException {
        var key = new ImmutablePair<>(bedId, roundedTime);
        synchronized (openArchives) {
            if (isShuttingDown) {
                logger.warn("Is shutting down, will not create any more archives");
                return null;
            }
            var currentStream = openArchives.get(key);
            if (currentStream != null) {
                // stream exists, return it
                openArchivesLastWritten.put(key, Instant.now());
                return currentStream;
            }

            // if stream didn't exist, start a new one and keep track of it
            BZip2CompressorOutputStream newOutputStream = makeNewArchive(bedId, roundedTime);

            openArchives.put(key, newOutputStream);
            openArchivesLastWritten.put(key, Instant.now());
            return newOutputStream;
        }
    }

    /**
     * Close archives if we haven't written to them for a minute or so.
     * A new one can always be opened if more data comes in.
     */
    @Scheduled(fixedDelay = 30000)
    public void closeStreamsNotRecentlyUsed() {
        Instant startTime = Instant.now();
        Instant mainLockAcquiredTime;
        List<BZip2CompressorOutputStream> toClose = new ArrayList<>();
        synchronized (openArchives) {
            mainLockAcquiredTime = Instant.now();
            logger.info("closeStreamsNotRecentlyUsed: |openArchives| = {}, |openArchivesLastWritten| = {}",
                    openArchives.size(), openArchivesLastWritten.size());
            var iterator = openArchives.entrySet().iterator();
            while (iterator.hasNext()) {
                var entry = iterator.next();
                var key = entry.getKey();
                Instant lastWritten = openArchivesLastWritten.get(key);
                if (lastWritten == null) {
                    logger.error("closeStreamsNotRecentlyUsed: stream {} has null last written value, this is weird so let's close it just in case",
                            key);
                } else {
                    Duration lastWriteDuration = Duration.between(lastWritten, Instant.now());
                    if (lastWriteDuration.getSeconds() > 60) {
                        logger.warn("closeStreamsNotRecentlyUsed: stream {} not written for {} seconds, queue for closure",
                                key, lastWriteDuration.getSeconds());
                    } else {
                        // keep it open
                        continue;
                    }
                }
                // Don't immediately close, but take out of action by removing from the map,
                // waiting for any writes to finish first (writes lock on the stream too)
                Instant preFileLock = Instant.now(), postFileLock;
                synchronized (entry.getValue()) {
                    postFileLock = Instant.now();
                    iterator.remove(); // only permissible way to remove during iteration
                    openArchivesLastWritten.remove(key);
                }
                long gapMillis = Duration.between(preFileLock, postFileLock).toMillis();
                if (gapMillis > 2) {
                    logger.warn("closeStreamsNotRecentlyUsed: SLOW ({} ms) file lock acquisition for {}", gapMillis, key);
                }
                // Because the bz2 block size is so large, the final close
                // can take quite a long time. We can't do it while we have
                // locked the entire data structure as it blocks ALL writes.
                toClose.add(entry.getValue());
            }
        }
        long gapMillis = Duration.between(startTime, mainLockAcquiredTime).toMillis();
        if (gapMillis > 2) {
            logger.warn("closeStreamsNotRecentlyUsed: SLOW ({} ms) main lock acquisition", gapMillis);
        }
        // Submit the slow file closes to single-thread executor.
        // Don't do it in the synchronized block in case we get
        // a "caller runs" rejection, because tying up the main
        // data structure is bad.
        // They have been removed from the main data structure so nothing
        // will attempt to use them while they're waiting to be closed.
        int queueSizeBefore = closeFileExecutor.getThreadPoolExecutor().getQueue().size();
        int queueCapacity = closeFileExecutor.getThreadPoolExecutor().getQueue().remainingCapacity() + queueSizeBefore;
        for (var stream : toClose) {
            closeFileExecutor.execute(() -> {
                try {
                    Instant preCloseTime = Instant.now();
                    stream.close();
                    long closeTimeMillis = Duration.between(preCloseTime, Instant.now()).toMillis();
                    logger.info("closeStreamsNotRecentlyUsed closed in {} ms", closeTimeMillis);
                } catch (IOException e) {
                    logger.error("closeStreamsNotRecentlyUsed - Error closing archive stream", e);
                }
            });
        }
        int queueSizeAfter = closeFileExecutor.getThreadPoolExecutor().getQueue().size();
        logger.info("closeStreamsNotRecentlyUsed: submitted {} tasks to queue (capacity {}), size went {} -> {}",
                toClose.size(), queueCapacity, queueSizeBefore, queueSizeAfter);
    }

    private Path buildArchivePath(String bedId, Instant roundedTime) {
        // hourly directories, minutely archive files
        String dateDir = HOURLY_DIR_FORMATTER
                .withZone(ZoneOffset.UTC)
                .format(roundedTime);

        // Eg. "20251030T1423Z"
        String timestampStr = DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmm'Z'")
                .withZone(ZoneOffset.UTC)
                .format(roundedTime);

        // The channel ID cannot be used in the path because there
        // are often multiple channels within a message.

        // Ensure uniqueness. If messages came out of order we might end up re-opening the archive
        // for an old one-minute slot, and we wouldn't want to overwrite it.
        String randomSuffix = String.format("%016x", new Random().nextLong());
        String fileName = String.format("%s_%s_%s.hl7archive.bz2", bedId, timestampStr, randomSuffix);
        return saveDirectory.resolve(dateDir).resolve(bedId).resolve(fileName);
    }

    BZip2CompressorOutputStream makeNewArchive(String bedId, Instant roundedTime) throws IOException {
        Path path = buildArchivePath(bedId, roundedTime);

        logger.warn("Creating new archive {}", path);
        // Ensure the directory exists
        Files.createDirectories(path.getParent());

        FileOutputStream fos = null;
        try {
            fos = new FileOutputStream(path.toFile());
            return new BZip2CompressorOutputStream(fos);
        } catch (IOException e) {
            if (fos != null) {
                fos.close();
            }
            throw e;
        }
    }

    /**
     * Add a message to the correct compressed archive.
     * @param messageText the message text itself
     * @param bedId location/bed ID
     * @param nowTime the time that under which to file the message
     * @throws IOException
     */
    public void saveMessage(String messageText, String bedId, Instant nowTime) throws IOException {
        logger.trace("JES: Starting HL7 message compression task");
        Instant time1 = Instant.now();
        Instant roundedTime = nowTime.truncatedTo(ChronoUnit.MINUTES);
        // this method call can be slow, presumably if it's waiting to acquire the lock
        BZip2CompressorOutputStream outputStream = getOutputStream(bedId, roundedTime);
        if (outputStream == null) {
            return;
        }
        Instant time2 = Instant.now();
        long gapGetHandle = Duration.between(time1, time2).toMillis();
        if (gapGetHandle > 2) {
            logger.info("saveMessage: slow getOutputStream: {} ms", gapGetHandle);
        }
        // multiple threads write to the same outputStream concurrently
        Instant time3, time4, time5;
        time3 = Instant.now();
        synchronized (outputStream) {
            time4 = Instant.now();
            outputStream.write(messageText.getBytes());
            // Put separator after each message (not just in between messages), so we can be sure
            // when unarchiving that the message didn't get truncated.
            outputStream.write(0x1c);
            time5 = Instant.now();
            // There is not much point in flushing here, because
            // we are limited by bzip2's large block size, so it's not going to be
            // flushed to disk anyway.
        }
        long gapWriteLock = Duration.between(time3, time4).toMillis();
        if (gapWriteLock > 2) {
            logger.info("saveMessage: slow write lock acquisition: {} ms", gapWriteLock);
        }
        long gapWrite = Duration.between(time4, time5).toMillis();
        if (gapWrite > 2) {
            logger.info("saveMessage: slow write: {} ms", gapWrite);
        }
    }

    /**
     * As part of shutdown, ensure all files are closed.
     * Spring will handle waiting for the executor tasks to complete due to
     * setWaitForTasksToCompleteOnShutdown(true) on the closeFileExecutor.
     */
    @PreDestroy
    public void closeAllFiles() {
        logger.warn("PreDestroy: Trying to close open HL7 archives in an orderly fashion");
        int archivesQueued = 0;
        synchronized (openArchives) {
            isShuttingDown = true;
            logger.warn("PreDestroy: JES0");
            var entries = openArchives.entrySet().iterator();
            while (entries.hasNext()) {
                logger.warn("PreDestroy: JES1");
                var entry = entries.next();
                BZip2CompressorOutputStream openStream = entry.getValue();
                entries.remove();
                logger.warn("PreDestroy: JES2");
                // Run the close in the current thread as the closeFileExecutor may
                // have been shut down
                try {
                    openStream.close();
                    logger.debug("Closed archive stream for {} during shutdown", entry.getKey());
                } catch (IOException e) {
                    logger.error("Could not gracefully close stream for {}",
                            entry.getKey(), e);
                }
                archivesQueued++;
                logger.warn("PreDestroy: JES3");
            }
        }
        logger.warn("PreDestroy: Closed {} open HL7 archives", archivesQueued);
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

