package uk.ac.ucl.rits.inform.datasources.waveform;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.test.context.ActiveProfiles;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test the Hl7MessageCompressor component.
 */
@ActiveProfiles("test")
class TestHl7MessageCompressor {
    private Path tempDir;
    private Hl7MessageSaver messageSaver;
    private Hl7MessageCompressor compressor;
    
    @BeforeEach
    void setup() throws IOException {
        // Create a fresh temp directory for each test
        tempDir = Files.createTempDirectory("test-compression-");
        
        // Create a message saver with our temp directory
        messageSaver = new Hl7MessageSaver(tempDir.toString());
        
        // Create compressor with 2 hours to keep uncompressed
        compressor = new Hl7MessageCompressor(messageSaver, true);
    }
    
    @Test
    void testCompressionDisabledWhenSavingDisabled() throws IOException {
        Hl7MessageSaver disabledSaver = new Hl7MessageSaver(null);
        Hl7MessageCompressor disabledCompressor = new Hl7MessageCompressor(disabledSaver, true);
        
        // Should not throw any errors, just do nothing
        disabledCompressor.compressOldMessages(Instant.now());
        
        assertEquals(0, disabledCompressor.getTotalArchivesCreated());
    }
    
    @Test
    void testCompressionDisabledByConfig() throws IOException {
        // Create compressor with compression explicitly disabled
        Hl7MessageCompressor disabledCompressor = new Hl7MessageCompressor(messageSaver, false);
        
        // Create some old files
        createTestMessages(3, "BED01");
        
        // Run compression
        disabledCompressor.compressOldMessages(Instant.now());
        
        // No archives should be created
        assertEquals(0, disabledCompressor.getTotalArchivesCreated());
    }
    
    @Test
    void testOldMessagesAreCompressed() throws IOException {
        // Create messages from 3 hours ago
        Instant threeHoursAgo = Instant.now().minusSeconds(3 * 3600);
        String bedId = "BED01";
        List<Path> createdFiles = createTestMessagesAtTime(threeHoursAgo, 5, bedId);
        
        // Verify files exist
        assertEquals(5, createdFiles.size());
        for (Path file : createdFiles) {
            assertTrue(Files.exists(file), "File should exist: " + file);
        }
        
        // Run compression
        compressor.compressOldMessages(Instant.now());
        
        // Verify archive was created
        assertEquals(1, compressor.getTotalArchivesCreated());
        assertEquals(5, compressor.getTotalFilesCompressed());
        
        // Verify original files were deleted
        for (Path file : createdFiles) {
            assertFalse(Files.exists(file), "Original file should be deleted: " + file);
        }
        
        // Verify archive exists and contains the correct files
        String hourDir =  Hl7MessageSaver.HOURLY_DIR_FORMATTER.format(threeHoursAgo.atZone(ZoneOffset.UTC));
        Path archivePath = tempDir.resolve(hourDir).resolve(bedId + ".tar.bz2");
        assertTrue(Files.exists(archivePath), "Archive should exist: " + archivePath);
        
        // Verify archive contents
        List<String> archiveEntries = listTarBz2Contents(archivePath);
        assertEquals(5, archiveEntries.size());
    }
    
    @Test
    void testRecentMessagesAreNotCompressed() throws IOException {
        // Create messages from 1 hour ago (less than the 2-hour threshold)
        Instant oneHourAgo = Instant.now().minusSeconds(3600);
        String bedId = "BED02";
        List<Path> createdFiles = createTestMessagesAtTime(oneHourAgo, 3, bedId);
        
        // Verify files exist
        assertEquals(3, createdFiles.size());
        
        // Run compression
        compressor.compressOldMessages(Instant.now());
        
        // Verify no archives were created
        assertEquals(0, compressor.getTotalArchivesCreated());
        
        // Verify original files still exist
        for (Path file : createdFiles) {
            assertTrue(Files.exists(file), "Recent file should not be deleted: " + file);
        }
    }
    
    @Test
    void testMultipleBedIdsCompressedSeparately() throws IOException {
        // Create messages for multiple beds from 3 hours ago
        Instant threeHoursAgo = Instant.now().minusSeconds(3 * 3600);
        
        List<Path> bed01Files = createTestMessagesAtTime(threeHoursAgo, 3, "BED01");
        List<Path> bed02Files = createTestMessagesAtTime(threeHoursAgo, 4, "BED02");
        List<Path> bed03Files = createTestMessagesAtTime(threeHoursAgo, 2, "BED03");
        
        // Run compression
        compressor.compressOldMessages(Instant.now());
        
        // Verify 3 archives were created (one per bed)
        assertEquals(3, compressor.getTotalArchivesCreated());
        assertEquals(9, compressor.getTotalFilesCompressed()); // 3 + 4 + 2
        
        // Verify all original files were deleted
        Stream.of(bed01Files, bed02Files, bed03Files).flatMap(List::stream).forEach(file -> {
            assertFalse(Files.exists(file), "Original file should be deleted: " + file);
        });
        
        // Verify archives exist with correct contents
        String hourDir =  Hl7MessageSaver.HOURLY_DIR_FORMATTER.format(threeHoursAgo.atZone(ZoneOffset.UTC));
        
        Path archive01 = tempDir.resolve(hourDir).resolve("BED01.tar.bz2");
        Path archive02 = tempDir.resolve(hourDir).resolve("BED02.tar.bz2");
        Path archive03 = tempDir.resolve(hourDir).resolve("BED03.tar.bz2");
        
        assertTrue(Files.exists(archive01));
        assertTrue(Files.exists(archive02));
        assertTrue(Files.exists(archive03));
        
        assertEquals(3, listTarBz2Contents(archive01).size());
        assertEquals(4, listTarBz2Contents(archive02).size());
        assertEquals(2, listTarBz2Contents(archive03).size());
    }
    
    @Test
    void testMultipleHourDirectoriesCompressed() throws IOException {
        // Create messages in different hour directories
        Instant threeHoursAgo = Instant.now().minusSeconds(3 * 3600);
        Instant fourHoursAgo = Instant.now().minusSeconds(4 * 3600);
        Instant fiveHoursAgo = Instant.now().minusSeconds(5 * 3600);
        
        createTestMessagesAtTime(threeHoursAgo, 2, "BED01");
        createTestMessagesAtTime(fourHoursAgo, 3, "BED01");
        createTestMessagesAtTime(fiveHoursAgo, 1, "BED01");
        
        // Run compression
        compressor.compressOldMessages(Instant.now());
        
        // Verify 3 archives were created (one per hour)
        assertEquals(3, compressor.getTotalArchivesCreated());
        assertEquals(6, compressor.getTotalFilesCompressed());
    }
    
    @Test
    void testEmptyDirectoriesAreDeleted() throws IOException {
        // Create messages from 3 hours ago
        Instant threeHoursAgo = Instant.now().minusSeconds(3 * 3600);
        String bedId = "BED01";
        List<Path> createdFiles = createTestMessagesAtTime(threeHoursAgo, 3, bedId);
        
        Path bedDir = createdFiles.get(0).getParent();
        Path hourDir = bedDir.getParent();
        
        assertTrue(Files.exists(bedDir));
        assertTrue(Files.exists(hourDir));
        
        // Run compression
        compressor.compressOldMessages(Instant.now());
        
        // Verify empty directories were deleted
        assertFalse(Files.exists(bedDir), "Empty bed directory should be deleted");
        assertFalse(Files.exists(hourDir), "Empty hour directory should be deleted");
    }
    
    @Test
    void testAlreadyCompressedFilesNotRecompressed() throws IOException {
        // Create and compress messages once
        Instant threeHoursAgo = Instant.now().minusSeconds(3 * 3600);
        createTestMessagesAtTime(threeHoursAgo, 3, "BED01");
        
        compressor.compressOldMessages(Instant.now());
        assertEquals(1, compressor.getTotalArchivesCreated());
        
        // Run compression again
        compressor.compressOldMessages(Instant.now());
        
        // Should still be 1 archive (not recreated)
        assertEquals(1, compressor.getTotalArchivesCreated());
    }
    
    @Test
    void testArchiveContentsAreCorrect() throws IOException {
        // Create messages with specific content
        Instant threeHoursAgo = Instant.now().minusSeconds(3 * 3600);
        String bedId = "BED01";
        
        String message1 = "MSH|^~\\&|TEST|||20251117120000||ORU^R01|msg1|P|2.5\r";
        String message2 = "MSH|^~\\&|TEST|||20251117120001||ORU^R01|msg2|P|2.5\r";
        String message3 = "MSH|^~\\&|TEST|||20251117120002||ORU^R01|msg3|P|2.5\r";
        
        messageSaver.saveMessage(message1, threeHoursAgo, bedId);
        messageSaver.saveMessage(message2, threeHoursAgo.plusSeconds(1), bedId);
        messageSaver.saveMessage(message3, threeHoursAgo.plusSeconds(2), bedId);
        
        // Run compression
        compressor.compressOldMessages(Instant.now());
        
        // Extract and verify archive contents
        String hourDir = Hl7MessageSaver.HOURLY_DIR_FORMATTER.format(threeHoursAgo.atZone(ZoneOffset.UTC));
        Path archivePath = tempDir.resolve(hourDir).resolve(bedId + ".tar.bz2");
        
        List<String> contents = extractTarBz2Contents(archivePath);
        assertEquals(3, contents.size());
        
        // All messages should be in the archive
        assertTrue(contents.stream().anyMatch(c -> c.contains("msg1")));
        assertTrue(contents.stream().anyMatch(c -> c.contains("msg2")));
        assertTrue(contents.stream().anyMatch(c -> c.contains("msg3")));
    }
    
    /**
     * Helper method to create test messages at the current time - 3 hours.
     */
    private List<Path> createTestMessages(int count, String bedId) throws IOException {
        Instant threeHoursAgo = Instant.now().minusSeconds(3 * 3600);
        return createTestMessagesAtTime(threeHoursAgo, count, bedId);
    }
    
    /**
     * Helper method to create test messages at a specific time.
     */
    private List<Path> createTestMessagesAtTime(Instant timestamp, int count, String bedId) throws IOException {
        List<Path> createdFiles = new ArrayList<>();
        
        for (int i = 0; i < count; i++) {
            String message = String.format("MSH|^~\\&|TEST|||%s||ORU^R01|msg%d|P|2.5\r",
                    DateTimeFormatter.ofPattern("yyyyMMddHHmmss").withZone(ZoneOffset.UTC).format(timestamp),
                    i);
            
            // Use the message saver to create files with proper structure
            messageSaver.saveMessage(message, timestamp.plusMillis(i * 100), bedId);
        }
        
        // Find the files that were created
        String hourDir =  Hl7MessageSaver.HOURLY_DIR_FORMATTER.format(timestamp.atZone(ZoneOffset.UTC));
        Path hourPath = tempDir.resolve(hourDir).resolve(bedId);
        
        if (Files.exists(hourPath)) {
            try (Stream<Path> files = Files.list(hourPath)) {
                createdFiles = files.filter(p -> p.toString().endsWith(".hl7")).toList();
            }
        }
        
        return createdFiles;
    }
    
    /**
     * Helper method to list contents of a tar.bz2 archive.
     */
    private List<String> listTarBz2Contents(Path archivePath) throws IOException {
        List<String> entries = new ArrayList<>();
        
        try (BufferedInputStream bis = new BufferedInputStream(Files.newInputStream(archivePath));
             BZip2CompressorInputStream bzIn = new BZip2CompressorInputStream(bis);
             TarArchiveInputStream tarIn = new TarArchiveInputStream(bzIn)) {
            
            TarArchiveEntry entry;
            while ((entry = tarIn.getNextTarEntry()) != null) {
                entries.add(entry.getName());
            }
        }
        
        return entries;
    }
    
    /**
     * Helper method to extract and read contents of a tar.bz2 archive.
     */
    private List<String> extractTarBz2Contents(Path archivePath) throws IOException {
        List<String> contents = new ArrayList<>();
        
        try (BufferedInputStream bis = new BufferedInputStream(Files.newInputStream(archivePath));
             BZip2CompressorInputStream bzIn = new BZip2CompressorInputStream(bis);
             TarArchiveInputStream tarIn = new TarArchiveInputStream(bzIn)) {
            
            TarArchiveEntry entry;
            while ((entry = tarIn.getNextTarEntry()) != null) {
                byte[] content = new byte[(int) entry.getSize()];
                tarIn.read(content);
                contents.add(new String(content));
            }
        }
        
        return contents;
    }
}

