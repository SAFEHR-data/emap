package uk.ac.ucl.rits.inform.datasources.waveform;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static uk.ac.ucl.rits.inform.datasources.waveform.Utils.readHl7FromResource;

/**
 * Test the file parsing aspects of {@link Hl7FromFile}.
 */
@SpringJUnitConfig
@SpringBootTest
@ActiveProfiles("test")
class TestHl7FromFileParsing {
    @Autowired
    private WaveformCollator waveformCollator;
    @Autowired
    private Hl7FromFile hl7FromFile;

    @BeforeEach
    void clearMessages() {
        waveformCollator.pendingMessages.clear();
    }

    /**
     * Test combinations:
     * Determine amounts of random whitespace. Do in uncompressed and compressed versions.
     */
    static List<Object[]> seedAndCompressProvider() {
        List<Object[]> params = new ArrayList<>();
        for (int seed = 1; seed <= 10; seed++) {
            params.add(new Object[] {seed, true});
            params.add(new Object[] {seed, false});
        }
        return params;
    }

    /**
     * Read HL7 messages from a FS (1c) delimited file.
     * Apply random whitespace as real messages seem to have this.
     */
    @ParameterizedTest
    @MethodSource("seedAndCompressProvider")
    void readAllFromFile(int seed, boolean compress, @TempDir Path tempDir) throws IOException, Hl7ParseException, WaveformCollator.CollationException, URISyntaxException {
        Path tempHl7DumpFile = tempDir.resolve("test_hl7.txt" + (compress ? ".bz2" : "" ));
        final int numHl7Messages = 10;
        makeTestFile(tempHl7DumpFile, numHl7Messages, new Random(seed), compress);
        hl7FromFile.readAndQueueAllMessagesFromFile(tempHl7DumpFile.toFile());
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

    private void makeTestFile(Path hl7File, int numMessages, Random random, boolean compress) throws IOException, URISyntaxException {
        BufferedOutputStream ostr = null;
        if (compress) {
            ostr = new BufferedOutputStream(new BZip2CompressorOutputStream(
                    new FileOutputStream(hl7File.toFile())));
        } else {
            ostr = new BufferedOutputStream(new FileOutputStream(hl7File.toFile()));
        }
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
