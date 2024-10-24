package uk.ac.ucl.rits.inform.datasources.waveform;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import uk.ac.ucl.rits.inform.datasources.waveform.hl7parse.Hl7ParseException;
import uk.ac.ucl.rits.inform.interchange.InterchangeValue;
import uk.ac.ucl.rits.inform.interchange.visit_observations.WaveformMessage;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static uk.ac.ucl.rits.inform.datasources.waveform.Utils.readHl7FromResource;

@SpringJUnitConfig
@SpringBootTest
@ActiveProfiles("test")
class TestHl7ParseAndSend {
    @Autowired
    private Hl7ParseAndSend hl7ParseAndSend;

    @Test
    void goodMessageSideRoom() throws IOException, URISyntaxException, Hl7ParseException {
        String hl7String = readHl7FromResource("hl7/test1.hl7");
        checkMessage(hl7String, "UCHT03ICURM08", "T03^T03 SR08^SR08-08");
    }

    @Test
    void goodMessageNormalBed() throws IOException, URISyntaxException, Hl7ParseException {
        String hl7String = readHl7FromResource("hl7/test1.hl7");
        String bed15 = "UCHT03ICUBED15";
        hl7String = hl7String.replaceAll("UCHT03ICURM08", bed15);
        checkMessage(hl7String, bed15, "T03^T03 BY01^BY01-15");
    }

    @Test
    void messageWithUnknownLocation() throws IOException, URISyntaxException, Hl7ParseException {
        String hl7String = readHl7FromResource("hl7/test1.hl7");
        hl7String = hl7String.replaceAll("UCHT03ICURM08", "UCHT03ICUSOMETHING");
        checkMessage(hl7String, "UCHT03ICUSOMETHING", null);
    }

    void checkMessage(String hl7String, String expectedSourceLocation, String expectedMappedLocation)
            throws IOException, URISyntaxException, Hl7ParseException {
        List<WaveformMessage> msgs = hl7ParseAndSend.parseHl7(hl7String);
        assertEquals(5, msgs.size());
        List<String> actualSource = msgs.stream().map(WaveformMessage::getSourceLocationString).distinct().toList();
        assertEquals(1, actualSource.size());
        assertEquals(expectedSourceLocation, actualSource.get(0));

        List<String> actualMapped = msgs.stream().map(WaveformMessage::getMappedLocationString).distinct().toList();
        assertEquals(1, actualMapped.size());
        assertEquals(expectedMappedLocation, actualMapped.get(0));
        assertEquals(
                List.of("52912", "52913", "27", "51911", "52921"),
                msgs.stream().map(WaveformMessage::getSourceStreamId).toList());
        assertEquals(
                List.of("Airway Volume Waveform", "Airway Pressure Waveform", "Generic ECG Waveform",
                        "O2 Pleth Waveform", "ETCO2"),
                msgs.stream().map(WaveformMessage::getMappedStreamDescription).toList());
        assertEquals(
                List.of(50, 50, 300, 100, 25),
                msgs.stream().map(WaveformMessage::getSamplingRate).toList());
        List<String> distinctMessageIds = msgs.stream().map(m -> m.getSourceMessageId()).distinct().toList();
        assertEquals(msgs.size(), distinctMessageIds.size());
        List<String> actualUnits = msgs.stream().map(WaveformMessage::getUnit).toList();
        assertEquals(List.of("mL", "cmH2O", "uV", "%", "%"), actualUnits);
        var expectedValues = List.of(
                List.of(42.10),
                List.of(42.20),
                List.of(42.30, 43.30, 44.30),
                List.of(42.40, 43.40, 44.40, 45.40),
                List.of(42.50, 43.50, 44.5, 45.5, 46.5));

        for (int i = 0; i < msgs.size(); i++) {
            WaveformMessage m = msgs.get(i);
            InterchangeValue<List<Double>> numericValues = m.getNumericValues();
            assertTrue(numericValues.isSave());
            List<Double> expected = expectedValues.get(i);
            assertEquals(expected, numericValues.get());
        }
    }

    @Test
    void messageWithMoreThanOneRepeat() throws IOException, URISyntaxException {
        String hl7String = readHl7FromResource("hl7/test1.hl7");
        String hl7WithReps = hl7String.replace("42.50^", "42.50~");
        Hl7ParseException e = assertThrows(Hl7ParseException.class, () -> hl7ParseAndSend.parseHl7(hl7WithReps));
        assertTrue(e.getMessage().contains("only be 1 repeat"));
    }

    @Test
    void messageWithConflictingLocation() throws IOException, URISyntaxException {
        String hl7String = readHl7FromResource("hl7/test1.hl7");
        String hl7WithReps = hl7String.replace("PV1||I|UCHT03ICURM08|", "PV1||I|UCHT03ICURM07|");
        Hl7ParseException e = assertThrows(Hl7ParseException.class, () -> hl7ParseAndSend.parseHl7(hl7WithReps));
        assertTrue(e.getMessage().contains("Unexpected location"));
    }

}
