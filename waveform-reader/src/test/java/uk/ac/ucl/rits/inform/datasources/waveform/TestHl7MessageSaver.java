package uk.ac.ucl.rits.inform.datasources.waveform;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test the Hl7MessageSaver component.
 */
class TestHl7MessageSaver {
    @Test
    void testNonExistentPath() {
        assertThrows(IllegalArgumentException.class,
                () -> {
                    Hl7MessageSaver saver = new Hl7MessageSaver("/non-existent-path");
                }
        );
    }

    @ParameterizedTest
    @NullAndEmptySource
    void testNullEmptyPath(String saveDirectoryPath) throws IOException {
        Hl7MessageSaver saver = new Hl7MessageSaver(saveDirectoryPath);
        assertFalse(saver.isSaveEnabled());
    }

    @Test
    void testSaveMessageCreatesHourlyDirectories(@TempDir Path tempDir) throws IOException {
        Hl7MessageSaver saver = new Hl7MessageSaver(tempDir.toString());
        assertEquals(tempDir, saver.getSaveDirectory());

        String testMessage = "MSH|^~\\&|TEST|||20251030142345||ORU^R01|12345|P|2.5\r";
        saver.saveMessage(testMessage, Instant.now(), "");

        assertEquals(1, saver.getSavedMessageCount());

        Files.exists(tempDir.resolve("20251030").resolve("14"));

        // Verify directory structure was created
        try (Stream<Path> paths = Files.walk(tempDir)) {
            List<Path> hl7Files = paths
                    .filter(Files::isRegularFile)
                    .filter(p -> p.toString().endsWith(".hl7"))
                    .collect(Collectors.toList());

            assertEquals(1, hl7Files.size());
            
            Path savedFile = hl7Files.get(0);
            String content = Files.readString(savedFile);
            assertEquals(testMessage, content);

            // Verify directory structure: {base}/{date}/{hour}/
            Path parent = savedFile.getParent(); // hour directory
            assertTrue(parent.getFileName().toString().matches("\\d{2}")); // hour is 2 digits
            
            Path grandparent = parent.getParent(); // date directory
            assertTrue(grandparent.getFileName().toString().matches("\\d{4}-\\d{2}-\\d{2}")); // date is YYYY-MM-DD
        }
    }

    @Test
    void testSaveMultipleMessages(@TempDir Path tempDir) throws IOException {
        Hl7MessageSaver saver = new Hl7MessageSaver(tempDir.toString());
        // should be grouped by hour
        record ExpectedFile(String bedId, String timeStamp) { }
        List<ExpectedFile> expectedFiles = List.of(
                new ExpectedFile("UCHT03ICURM08", "142345"),
                new ExpectedFile("UCHT03ICURM09", "142525"),
                new ExpectedFile("UCHT03ICURM08", "144458"),
                new ExpectedFile("UCHT03ICURM08", "163311"),
                new ExpectedFile("UCHT03ICURM08", "165527")
        );

        String dateString = "20251030";
        for (int i = 0; i < expectedFiles.size(); i++) {
            String message = String.format(
                    "MSH|^~\\&|TEST|||%s%s||ORU^R01|message%s|P|2.5\r"
                            + "PID|\r"
                            + "PV1||I|%s|\r",
                    dateString, expectedFiles.get(i).timeStamp, i, expectedFiles.get(i).bedId);
            saver.saveMessage(message, Instant.now(), "");
        }

        assertEquals(5, saver.getSavedMessageCount());

        // Verify all files were saved
        try (Stream<Path> paths = Files.walk(tempDir)) {
            Set<Path> actualHl7Files = paths
                    .filter(Files::isRegularFile)
                    .map(f -> tempDir.relativize(f))
                    .collect(Collectors.toSet());

            // all files end .hl7
            for (ExpectedFile expectedFile : expectedFiles) {
                // get just the hours
                String timeBucket = expectedFile.timeStamp.substring(0, 2);
                String expectedFilePath = String.format("%sT%s/%s/%sT%s.hl7",
                        dateString, timeBucket, expectedFile.bedId, dateString, expectedFile.timeStamp);
                assertTrue(actualHl7Files.contains(expectedFilePath));
            }

            // all files got created, and nothing more
            assertEquals(5, actualHl7Files.size());
        }
    }

    @Test
    void testSaveEmptyMessageIsSkipped(@TempDir Path tempDir) throws IOException {
        Hl7MessageSaver saver = new Hl7MessageSaver(tempDir.toString());

        saver.saveMessage("", Instant.now(), "");
        saver.saveMessage(null, Instant.now(), "");

        assertEquals(0, saver.getSavedMessageCount());

        // Verify no files were saved
        try (Stream<Path> paths = Files.walk(tempDir)) {
            long fileCount = paths
                    .filter(Files::isRegularFile)
                    .filter(p -> p.toString().endsWith(".hl7"))
                    .count();

            assertEquals(0, fileCount);
        }
    }

    @Test
    void testFilenamesAreUnique(@TempDir Path tempDir) throws IOException {
        Hl7MessageSaver saver = new Hl7MessageSaver(tempDir.toString());

        // Save messages rapidly to test uniqueness
        for (int i = 0; i < 10; i++) {
            saver.saveMessage("message " + i, Instant.now(), "");
        }

        try (Stream<Path> paths = Files.walk(tempDir)) {
            List<String> filenames = paths
                    .filter(Files::isRegularFile)
                    .filter(p -> p.toString().endsWith(".hl7"))
                    .map(p -> p.getFileName().toString())
                    .collect(Collectors.toList());

            // All filenames should be unique
            assertEquals(10, filenames.size());
            assertEquals(10, filenames.stream().distinct().count());
        }
    }

    @Test
    void testSaveMessageWithSpecialCharacters(@TempDir Path tempDir) throws IOException {
        Hl7MessageSaver saver = new Hl7MessageSaver(tempDir.toString());

        String specialMessage = "MSH|^~\\&|TEST|||20251030142345||ORU^R01|12345|P|2.5\r\n"
                + "OBX|1|NM|HR^Heart Rate||120|bpm|||||F\r\n"
                + "OBX|2|NM|SpO2^Oxygen Saturation||98|%|||||F\r";
        
        saver.saveMessage(specialMessage, Instant.now(), "");

        try (Stream<Path> paths = Files.walk(tempDir)) {
            List<Path> hl7Files = paths
                    .filter(Files::isRegularFile)
                    .filter(p -> p.toString().endsWith(".hl7"))
                    .collect(Collectors.toList());

            assertEquals(1, hl7Files.size());
            String content = Files.readString(hl7Files.get(0));
            assertEquals(specialMessage, content);
        }
    }
}

