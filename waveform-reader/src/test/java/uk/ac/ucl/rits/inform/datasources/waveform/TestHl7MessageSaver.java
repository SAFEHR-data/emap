package uk.ac.ucl.rits.inform.datasources.waveform;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import uk.ac.ucl.rits.inform.datasources.waveform.hl7parse.Hl7ParseException;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

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
        registry.add("waveform.hl7.save.enabled", () -> true);
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
                ) {
            String expectedFilePathRegex() {
                // File name has a random string to make it unique, so need a regex to match its expected value.
                String timeBucket = this.expectedUtcTimeStamp.substring(0, 2); // just the hours
                return String.format("%sT%s/%s/%s_%sT%s_\\w{16}.hl7archive.bz2",
                        this.expectedUtcDate, timeBucket,
                        this.bedId,
                        this.bedId, this.expectedUtcDate, this.expectedUtcRoundedTimeStamp);
            }
        }
        /**
         * Here we are testing:
         *   - two messages in the same archive
         *   - conversion to UTC during GMT and BST
         *   - rounding to 4 minute time slot
         *   - different day/room/hour directories
         */
        List<ExpectedFile> expectedFiles = List.of(
                new ExpectedFile("UCHT03ICURM08", "000045.123+0100", "20251026", "230045.123Z", "2300Z", "20251025"),
                new ExpectedFile("UCHT03ICURM09", "003025.125+0100", "20251026", "233025.125Z", "2328Z", "20251025"),
                new ExpectedFile("UCHT03ICURM08", "004458.126+0100", "20251026", "234458.126Z", "2344Z", "20251025"),
                new ExpectedFile("UCHT03ICURM08", "004658.126+0100", "20251026", "234658.126Z", "2344Z", "20251025"),
                new ExpectedFile("UCHT03ICURM08", "010211.127+0100", "20251026", "000211.127Z", "0000Z", "20251026"),
                new ExpectedFile("UCHT03ICURM08", "014511.127+0100", "20251026", "004511.127Z", "0044Z", "20251026"),
                new ExpectedFile("UCHT03ICURM08", "013511.127+0000", "20251026", "013511.127Z", "0132Z", "20251026")
        );

        // what each file should contain (to be built up as we send the messages)
        Map<String, StringBuilder> expectedFileToContents = expectedFiles.stream()
                .collect(Collectors.toMap(
                        ExpectedFile::expectedFilePathRegex,
                        ef -> new StringBuilder(),
                        (v1, v2) -> v1
                ));
        for (int i = 0; i < expectedFiles.size(); i++) {
            final ExpectedFile expectedFile = expectedFiles.get(i);
            // We should have a final \r here, but it gets stripped off in the message save and makes the test fail
            String message = String.format(
                    "MSH|^~\\&|DATACAPTOR||||%s%s||ORU^R01|message%s|P|2.5\r"
                            + "PID|\r"
                            + "PV1||I|%s|",
                    expectedFile.messageDate, expectedFile.messageTimeStamp, i,
                    expectedFile.bedId);
            StringBuilder expectedContents = expectedFileToContents.get(expectedFile.expectedFilePathRegex());
            expectedContents.append(message);
            expectedContents.append("\u001c");
            // writes to filesystem
            hl7ParseAndQueue.saveParseQueue(message, true);
        }
        // Normally the scheduled method would close off inactive bz2 files for us, but
        // scheduler is disabled during unit tests, and in any case we want it to happen NOW, so do this manually.
        hl7MessageCompressor.closeAllBz2Files("unit test");

        // Verify all files were saved and contain the right thing
        try (Stream<Path> paths = Files.walk(tempDir)) {
            Set<Path> actualHl7Files = paths
                    .filter(Files::isRegularFile)
                    .map(f -> tempDir.relativize(f))
                    .collect(Collectors.toSet());

            // all files are bz2 archives but may contain more than one of the original HL7 files
            for (ExpectedFile expectedFile : expectedFiles) {
                String expectedFilePathRegex = expectedFile.expectedFilePathRegex();
                Pattern pattern = Pattern.compile(expectedFilePathRegex);
                List<Path> matchingFiles = actualHl7Files.stream().filter(p -> pattern.matcher(p.toString()).matches()).toList();
                // exactly one matching file
                assertEquals(1, matchingFiles.size(),
                        "exp regex: " + expectedFilePathRegex.toString() + ", not found in: " + actualHl7Files.toString());
                // open the bz2 file and check the contents
                Path bz2Path = tempDir.resolve(matchingFiles.get(0));
                try (InputStream is = new BufferedInputStream(new BZip2CompressorInputStream(new FileInputStream(bz2Path.toFile())))) {
                    String actualContents = new String(is.readAllBytes());
                    String expectedContents = expectedFileToContents.get(expectedFile.expectedFilePathRegex()).toString();
                    assertEquals(expectedContents, actualContents);
                }
            }

            // all files got created, and nothing more
            assertEquals(6, actualHl7Files.size());
        }
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


}

