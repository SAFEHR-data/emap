package uk.ac.ucl.rits.inform.datasources.waveform;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test the file scanning aspects of {@link Hl7FromFile}.
 */
@SpringJUnitConfig
@SpringBootTest
@ActiveProfiles("test")
class TestHl7FromFileScanning {
    @Autowired
    private Hl7FromFile hl7FromFile;

    private static final Path bz2TempDir;
    private static final List<String> allBz2Files;

    // testScanFiles is a read-only test, so we can set this up once at the beginning
    static {
        try {
            bz2TempDir = Files.createTempDirectory("file-scan-test");
            System.out.println("bz2TempDir = " + bz2TempDir);

            List<String> allFiles =
                    List.of(
                            "20240102T12/UCHT03ICURM01/UCHT03ICURM01_20240102T1200Z_5f13ab6abda78647.hl7archive.bz2",
                            "20240102T12/UCHT03ICURM06/UCHT03ICURM06_20240102T1200Z_5f13ab6abda78647.hl7archive.bz2",
                            "20240102T12/UCHT03ICURM06/UCHT03ICURM06_20240103T1200Z_5f13ab6abda78647.hl7archive.bz2"
                    );
            // make some bz2 (empty because we only filter based on file name anyway)
            for (String file : allFiles) {
                Path absPath = bz2TempDir.resolve(Path.of(file));
                absPath.getParent().toFile().mkdirs();
                // if it already existed something has gone wrong
                assertTrue(absPath.toFile().createNewFile());
            }
            allBz2Files = allFiles;
        } catch (IOException e) {
            throw new RuntimeException("Failed to create bz2 scan test data", e);
        }
    }

    @DynamicPropertySource
    static void registerProperties(DynamicPropertyRegistry registry) {
        registry.add("waveform.hl7.save.directory", () -> bz2TempDir.toString());
    }

    static List<Object[]> fileScanTests() {
        List<Object[]> params = List.of(
                // test inclusive and exclusive intervals
                new Object[]{
                        Instant.parse("2024-01-02T12:00:00Z"), Instant.parse("2024-01-02T13:00:00Z"), "UCHT03ICURM01",
                        List.of(0)},
                new Object[]{
                        Instant.parse("2024-01-02T12:00:01Z"), Instant.parse("2024-01-02T13:00:00Z"), "UCHT03ICURM01",
                        List.of()},
                new Object[]{
                        Instant.parse("2024-01-02T11:00:00Z"), Instant.parse("2024-01-02T12:00:00Z"), "UCHT03ICURM01",
                        List.of()},
                // get all locations for the same interval
                new Object[]{
                        Instant.parse("2024-01-01T12:00:00Z"), Instant.parse("2024-01-02T13:00:00Z"), null,
                        List.of(0, 1)}
        );
        return params;
    }

    @ParameterizedTest
    @MethodSource("fileScanTests")
    void testScanFiles(Instant start, Instant end, String sourceLocation, List<Integer> expectedFileIndexes) throws IOException {
        List<File> filteredFiles = hl7FromFile.scanFiles(start, end, sourceLocation);
        List<File> expectedFiles = new ArrayList<>();
        for (Integer fileIndex : expectedFileIndexes) {
            expectedFiles.add(bz2TempDir.resolve(allBz2Files.get(fileIndex)).toFile());
        }
        assertEquals(expectedFiles, filteredFiles);
    }
}
