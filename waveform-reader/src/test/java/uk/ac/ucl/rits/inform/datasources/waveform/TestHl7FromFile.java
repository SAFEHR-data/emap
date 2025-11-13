package uk.ac.ucl.rits.inform.datasources.waveform;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import uk.ac.ucl.rits.inform.datasources.waveform.hl7parse.Hl7ParseException;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static uk.ac.ucl.rits.inform.datasources.waveform.Utils.readHl7FromResource;

@SpringJUnitConfig
@SpringBootTest
@ActiveProfiles("test")
class TestHl7FromFile {
    @Autowired
    private Hl7ParseAndQueue hl7ParseAndQueue;
    @Autowired
    private WaveformCollator waveformCollator;
    @Autowired
    private Hl7FromFile hl7FromFile;

    @BeforeEach
    void clearMessages() {
        waveformCollator.pendingMessages.clear();
    }

    static IntStream ints() {
        return IntStream.rangeClosed(1, 10);
    }

    /**
     * Read HL7 messages from a FS (1c) delimited file.
     * Apply random whitespace as real messages seem to have this.
     */
    @ParameterizedTest
    @MethodSource({"ints"})
    void readAllFromFile(int seed, @TempDir Path tempDir) throws IOException, Hl7ParseException, WaveformCollator.CollationException, URISyntaxException {
        Path tempHl7DumpFile = tempDir.resolve("test_hl7.txt");
        final int numHl7Messages = 10;
        makeTestFile(tempHl7DumpFile, numHl7Messages, new Random(seed));
        hl7FromFile.readOnceAndQueue(tempHl7DumpFile.toFile());
        final int messagesPerHl7 = 5;
        assertEquals(numHl7Messages * messagesPerHl7, waveformCollator.getPendingMessageCount());
    }

    private List<Byte> randomWhitespaceSurrounding(byte surrounded, Random random) {
        int numCRs = random.nextInt(0, 3);
        List<Byte> allBytes = new ArrayList<>();
        allBytes.add(surrounded);
        allBytes.addAll(Collections.nCopies(numCRs, (byte)0x0d));
        Collections.shuffle(allBytes, random);
        return allBytes;
    }

    private void makeTestFile(Path hl7File, int numMessages, Random random) throws IOException, URISyntaxException {
        BufferedOutputStream ostr = new BufferedOutputStream(new FileOutputStream(hl7File.toFile()));
        String hl7Source = readHl7FromResource("hl7/test1.hl7");
        // space timestamps one second apart (they can't be the same or the collator will complain)
        Long cludgyDate = 20240731142108L;
        for (int i = 0; i < numMessages; i++) {
            String thisHl7 = hl7Source.replaceAll(cludgyDate.toString(), Long.valueOf(cludgyDate + i).toString());

            for (byte b: randomWhitespaceSurrounding((byte) 0x0b, random)) {
                ostr.write(b);
            }
            ostr.write(thisHl7.getBytes(StandardCharsets.UTF_8));
            for (byte b: randomWhitespaceSurrounding((byte) 0x1c, random)) {
                ostr.write(b);
            }
        }
        ostr.close();
    }

    /**
     * Test reading HL7 messages from a directory tree structure.
     */
    @Test
    void testReadFromDirectory(@TempDir Path tempDir) throws IOException, URISyntaxException {
        // Create directory structure with messages
        String hl7Content = readHl7FromResource("hl7/test1.hl7");
        
        // Create messages in multiple hours
        Path day1 = tempDir.resolve("2025-10-30");
        Path hour1 = day1.resolve("14");
        Path hour2 = day1.resolve("15");
        Files.createDirectories(hour1);
        Files.createDirectories(hour2);
        
        // Write some test files
        Files.writeString(hour1.resolve("message1.hl7"), hl7Content.replace("20240731142108", "20240731142108"));
        Files.writeString(hour1.resolve("message2.hl7"), hl7Content.replace("20240731142108", "20240731142109"));
        Files.writeString(hour2.resolve("message3.hl7"), hl7Content.replace("20240731142108", "20240731142110"));
        
        // Read from directory
        List<String> messages = hl7FromFile.readFromDirectory(tempDir.toFile());
        
        assertEquals(3, messages.size());
        for (String msg : messages) {
            assertTrue(msg.contains("MSH"));
        }
    }

    /**
     * Test reading from directory processes files in chronological order.
     */
    @Test
    void testReadFromDirectoryInOrder(@TempDir Path tempDir) throws IOException, URISyntaxException {
        String hl7Content = readHl7FromResource("hl7/test1.hl7");
        
        // Create multiple days and hours
        Path day1 = tempDir.resolve("2025-10-29");
        Path day2 = tempDir.resolve("2025-10-30");
        Path day1hour1 = day1.resolve("23");
        Path day2hour1 = day2.resolve("00");
        Path day2hour2 = day2.resolve("01");
        Files.createDirectories(day1hour1);
        Files.createDirectories(day2hour1);
        Files.createDirectories(day2hour2);
        
        // Write files in non-chronological order (to test sorting)
        Files.writeString(day2hour2.resolve("2025-10-30T01-00-02.000Z_aaa_000003.hl7"), 
                hl7Content.replace("20240731142108", "20240731142112"));
        Files.writeString(day1hour1.resolve("2025-10-29T23-59-58.000Z_aaa_000001.hl7"), 
                hl7Content.replace("20240731142108", "20240731142108"));
        Files.writeString(day2hour1.resolve("2025-10-30T00-00-00.000Z_aaa_000002.hl7"), 
                hl7Content.replace("20240731142108", "20240731142110"));
        
        List<String> messages = hl7FromFile.readFromDirectory(tempDir.toFile());
        
        assertEquals(3, messages.size());
        // Verify they're in chronological order by checking timestamps
        assertTrue(messages.get(0).contains("20240731142108"));
        assertTrue(messages.get(1).contains("20240731142110"));
        assertTrue(messages.get(2).contains("20240731142112"));
    }

    /**
     * Test that readOnceAndQueue works with both file and directory inputs.
     */
    @Test
    void testReadOnceAndQueueWithDirectory(@TempDir Path tempDir) throws IOException, URISyntaxException, Hl7ParseException, WaveformCollator.CollationException {
        // Create directory with messages
        String hl7Content = readHl7FromResource("hl7/test1.hl7");
        Path day1 = tempDir.resolve("2025-10-30");
        Path hour1 = day1.resolve("14");
        Files.createDirectories(hour1);
        
        final int numMessages = 5;
        for (int i = 0; i < numMessages; i++) {
            String modifiedHl7 = hl7Content.replace("20240731142108", String.valueOf(20240731142108L + i));
            Files.writeString(hour1.resolve("message" + i + ".hl7"), modifiedHl7);
        }
        
        // Clear before test
        waveformCollator.pendingMessages.clear();
        
        // Read and queue from directory
        hl7FromFile.readOnceAndQueue(tempDir.toFile());
        
        final int messagesPerHl7 = 5;
        assertEquals(numMessages * messagesPerHl7, waveformCollator.getPendingMessageCount());
    }

    /**
     * Test that empty files are skipped when reading from directory.
     */
    @Test
    void testReadFromDirectorySkipsEmptyFiles(@TempDir Path tempDir) throws IOException, URISyntaxException {
        String hl7Content = readHl7FromResource("hl7/test1.hl7");
        
        Path day1 = tempDir.resolve("2025-10-30");
        Path hour1 = day1.resolve("14");
        Files.createDirectories(hour1);
        
        Files.writeString(hour1.resolve("message1.hl7"), hl7Content);
        Files.writeString(hour1.resolve("empty.hl7"), "");
        Files.writeString(hour1.resolve("whitespace.hl7"), "   \n\t  ");
        Files.writeString(hour1.resolve("message2.hl7"), hl7Content);
        
        List<String> messages = hl7FromFile.readFromDirectory(tempDir.toFile());
        
        // Should only get the 2 valid messages
        assertEquals(2, messages.size());
    }

}
