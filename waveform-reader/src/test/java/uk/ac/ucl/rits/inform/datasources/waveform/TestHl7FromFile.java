package uk.ac.ucl.rits.inform.datasources.waveform;

import org.junit.jupiter.api.BeforeEach;
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
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static uk.ac.ucl.rits.inform.datasources.waveform.Utils.readHl7FromResource;

@SpringJUnitConfig
@SpringBootTest
@ActiveProfiles("test")
class TestHl7FromFile {
    @Autowired
    private Hl7ParseAndSend hl7ParseAndSend;
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

}
