package uk.ac.ucl.rits.inform.datasources.waveform;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import uk.ac.ucl.rits.inform.datasources.waveform.hl7parse.Hl7ParseException;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test the Hl7MessageSaver component.
 */
@SpringJUnitConfig
@SpringBootTest
@ActiveProfiles("test")
class TestHl7MessageSaver {
    @Autowired
    Hl7ParseAndQueue hl7ParseAndQueue;
    @Autowired
    Hl7MessageSaver messageSaver;
    @Autowired
    Hl7MessageCompressor hl7MessageCompressor;

    // temp dir needs to be calculated before Spring reads the property
    private static final Path tempDir;
    static {
        try {
            tempDir = Files.createTempDirectory("test-storage-");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @DynamicPropertySource
    static void registerProperties(DynamicPropertyRegistry registry) {
        registry.add("waveform.hl7.save.directory", () -> tempDir.toString());
        registry.add("waveform.hl7.save.archive_time_slot_minutes", () -> 4);
    }

    /**
     * Because Spring only gets this temp dir once at init time, we have to reuse it.
     * Delete contents of temp dir but not temp dir itself.
     */
    @BeforeEach
    void clearTempDir() throws IOException {
        List<Path> allFiles;
        try (Stream<Path> walk = Files.walk(tempDir)) {
            allFiles = walk.toList();
        }
        // Delete in reverse depth-first order to guarantee
        // that directories will be empty when we come to delete them.
        for (int i = allFiles.size() - 1; i >= 0; i--) {
            Path path = allFiles.get(i);
            // .walk includes the root dir, which we don't want to delete
            if (!path.equals(tempDir)) {
                path.toFile().delete();
            }
        }
        try (Stream<Path> walk = Files.walk(tempDir)) {
            List<Path> newList = walk.toList();
            assertEquals(1, newList.size());
            assertEquals(newList.get(0), tempDir);
        }
    }

    @Test
    void testNonExistentPath() {
        assertThrows(IllegalArgumentException.class,
                () -> {
                    Hl7MessageSaver saver = new Hl7MessageSaver(true, "/non-existent-path", hl7MessageCompressor);
                }
        );
    }

    @ParameterizedTest
    @NullAndEmptySource
    void testNullEmptyPath(String saveDirectoryPath) throws IOException {
        Hl7MessageSaver saver = new Hl7MessageSaver(true, saveDirectoryPath, hl7MessageCompressor);
        assertFalse(saver.isSaveEnabled());
    }


    @Test
    void testSaveMultipleMessages() throws IOException, WaveformCollator.CollationException, Hl7ParseException {
        // should be grouped by hour
        record ExpectedFile(
                String bedId,
                String messageTimeStamp,
                String messageDate, // (local time with offset specified)
                String expectedUtcTimeStamp,
                String expectedUtcRoundedTimeStamp,
                String expectedUtcDate
                ) { }
        // While we're at it, let's choose the most awkward timestamps ;)
        List<ExpectedFile> expectedFiles = List.of(
                new ExpectedFile("UCHT03ICURM08", "000045.123+0100", "20251026", "230045.123Z", "2300", "20251025"),
                new ExpectedFile("UCHT03ICURM09", "003025.125+0100", "20251026", "233025.125Z", "2328", "20251025"),
                new ExpectedFile("UCHT03ICURM08", "004458.126+0100", "20251026", "234458.126Z", "2344", "20251025"),
                new ExpectedFile("UCHT03ICURM08", "010211.127+0100", "20251026", "000211.127Z", "0000", "20251026"),
                new ExpectedFile("UCHT03ICURM08", "014511.127+0100", "20251026", "004511.127Z", "0044", "20251026"),
                new ExpectedFile("UCHT03ICURM08", "013511.127+0000", "20251026", "013511.127Z", "0132", "20251026")
        );

        for (int i = 0; i < expectedFiles.size(); i++) {
            String message = String.format(
                    "MSH|^~\\&|DATACAPTOR||||%s%s||ORU^R01|message%s|P|2.5\r"
                            + "PID|\r"
                            + "PV1||I|%s|\r",
                    expectedFiles.get(i).messageDate, expectedFiles.get(i).messageTimeStamp, i,
                    expectedFiles.get(i).bedId);
            // writes to filesystem too
            hl7ParseAndQueue.saveParseQueue(message, true);
        }

        // Verify all files were saved
        try (Stream<Path> paths = Files.walk(tempDir)) {
            Set<Path> actualHl7Files = paths
                    .filter(Files::isRegularFile)
                    .map(f -> tempDir.relativize(f))
                    .collect(Collectors.toSet());

            // all files are bz2 archives but may contain more than one of the original HL7 files
            for (ExpectedFile expectedFile : expectedFiles) {
                // get just the hours
                String timeBucket = expectedFile.expectedUtcTimeStamp.substring(0, 2);
                // use regex to match the file name regardless of unique-ifying string
                String expectedFilePathRegex = String.format("%sT%s/%s/%s_%sT%s_\\w{16}.hl7archive.bz2",
                        expectedFile.expectedUtcDate, timeBucket,
                        expectedFile.bedId,
                        expectedFile.bedId, expectedFile.expectedUtcDate, expectedFile.expectedUtcRoundedTimeStamp);
                Pattern pattern = Pattern.compile(expectedFilePathRegex);
                assertTrue(actualHl7Files.stream().anyMatch(p -> pattern.matcher(p.toString()).matches()),
                        "exp regex: " + expectedFilePathRegex.toString() + ", not found in: " + actualHl7Files.toString());
            }

            // all files got created, and nothing more
            assertEquals(6, actualHl7Files.size());
        }
    }

    @Test
    void testSaveMultipleMessagesWithCompression() throws IOException, WaveformCollator.CollationException, Hl7ParseException {
        testSaveMultipleMessages();
//        Instant compressTime = Instant.parse("2025-10-26T00:01:00.000Z");
        checkExpectedArchives();
    }

    private void checkExpectedArchives() {

    }

    @Test
    void testNullParam1() {
        assertThrows(NullPointerException.class, () -> {
                    messageSaver.saveMessage(null, Instant.now(), "foo");
                }
        );
    }
    @Test
    void testNullParam2() {
        assertThrows(NullPointerException.class, () -> {
                    messageSaver.saveMessage("bar", null, "foo");
                }
        );
    }
    @Test
    void testNullParam3() {
        assertThrows(NullPointerException.class, () -> {
                    messageSaver.saveMessage("foo", Instant.now(), null);
                }
        );
    }

    @Test
    void testFilenamesAreUnique() throws IOException {
        // Save messages, some of which have the same timestamps
        Instant now = Instant.now();
        for (int i = 0; i < 10; i++) {
            messageSaver.saveMessage("message " + i, now.plusMillis(i / 3), "foo");
        }

        try (Stream<Path> paths = Files.walk(tempDir)) {
            List<String> filenames = paths
                    .filter(Files::isRegularFile)
                    .filter(p -> p.toString().endsWith(".hl7archive.bz2"))
                    .map(p -> p.getFileName().toString())
                    .toList();

            // All filenames should be unique
            assertEquals(10, filenames.size());
            assertEquals(10, filenames.stream().distinct().count());
        }
    }

}

