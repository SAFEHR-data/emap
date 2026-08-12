package uk.ac.ucl.rits.inform.datasources.waveform;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import uk.ac.ucl.rits.inform.datasources.waveform.hl7parse.Hl7ParseException;
import uk.ac.ucl.rits.inform.interchange.InterchangeValue;
import uk.ac.ucl.rits.inform.interchange.visit_observations.WaveformBaseMessage;
import uk.ac.ucl.rits.inform.interchange.visit_observations.WaveformLowFreqMessage;
import uk.ac.ucl.rits.inform.interchange.visit_observations.WaveformMessage;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static uk.ac.ucl.rits.inform.datasources.waveform.Utils.readHl7FromResource;

@SpringJUnitConfig
@SpringBootTest
@ActiveProfiles("test")
class TestHl7ParseAndQueue {
    @Autowired
    private Hl7ParseAndQueue hl7ParseAndQueue;

    @Test
    void goodMessageSideRoom() throws IOException, URISyntaxException, Hl7ParseException {
        String hl7String = readHl7FromResource("hl7/test1.hl7");
        checkWaveformMessage(hl7String, "UCHT03ICURM08", "T03^T03 SR08^SR08-08");
    }

    @Test
    void goodMessageNormalBed() throws IOException, URISyntaxException, Hl7ParseException {
        String hl7String = readHl7FromResource("hl7/test1.hl7");
        String bed15 = "UCHT03ICUBED15";
        hl7String = hl7String.replaceAll("UCHT03ICURM08", bed15);
        checkWaveformMessage(hl7String, bed15, "T03^T03 BY01^BY01-15");
    }

    @Test
    void messageWithUnknownLocation() throws IOException, URISyntaxException, Hl7ParseException {
        String hl7String = readHl7FromResource("hl7/test1.hl7");
        hl7String = hl7String.replaceAll("UCHT03ICURM08", "UCHT03ICUSOMETHING");
        checkWaveformMessage(hl7String, "UCHT03ICUSOMETHING", null);
    }

    void checkWaveformMessage(String hl7String, String expectedSourceLocation, String expectedMappedLocation)
            throws IOException, URISyntaxException, Hl7ParseException {
        List<WaveformMessage> msgs = makeMessagesAndBasicChecks(hl7String, expectedSourceLocation, expectedMappedLocation, 5).stream().map(m -> (WaveformMessage)m).toList();
        assertEquals(
                List.of("52912", "52913", "27", "51911", "52921"),
                msgs.stream().map(WaveformMessage::getSourceVariableId).toList());
        assertEquals(
                List.of("Airway Volume Waveform", "Airway Pressure Waveform", "Generic ECG Waveform",
                        "O2 Pleth Waveform", "ETCO2"),
                msgs.stream().map(WaveformMessage::getMappedVariableDescription).toList());
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

    private List<WaveformBaseMessage> makeMessagesAndBasicChecks(String hl7String, String expectedSourceLocation, String expectedMappedLocation, int expectedMessageCount) throws Hl7ParseException {
        List<WaveformBaseMessage> msgs = hl7ParseAndQueue.parseHl7Fully(hl7ParseAndQueue.parseHl7Headers(hl7String)).waveformBaseMessages();
        assertEquals(expectedMessageCount, msgs.size());
        List<String> actualSource = msgs.stream().map(WaveformBaseMessage::getSourceLocationString).distinct().toList();
        assertEquals(1, actualSource.size());
        assertEquals(expectedSourceLocation, actualSource.get(0));

        List<String> actualMapped = msgs.stream().map(WaveformBaseMessage::getMappedLocationString).distinct().toList();
        assertEquals(1, actualMapped.size());
        assertEquals(expectedMappedLocation, actualMapped.get(0));
        return msgs;
    }

    /**
     * Represent expecte values for the main fields in a {@link WaveformLowFreqMessage}
     * @param variableId variable ID as a string
     * @param sourceValue the source (unmapped) value as a string
     * @param mappedUnits the units as a string (eg. "cmH2O")
     * @param numericValue value if a numeric conversion is expected, else null
     * @param stringValue value if a string (incl categorical) conversion is expected, else null
     */
    record ExpectedWaveformMessage(String variableId, String sourceValue, String mappedUnits, Double numericValue, String stringValue) {
        public void assertIsEqual(WaveformLowFreqMessage actualMessage) {
            assertEquals(variableId, actualMessage.getSourceVariableId());
            assertEquals(sourceValue, actualMessage.getSourceValue().get());
            if (numericValue == null) {
                assertTrue(actualMessage.getNumericValue().isUnknown());
            } else {
                assertEquals(numericValue, actualMessage.getNumericValue().get());
            }
            if (stringValue == null) {
                assertTrue(actualMessage.getStringValue().isUnknown());
            } else {
                assertEquals(stringValue, actualMessage.getStringValue().get());
            }
        }
        static void checkAllMessages(
                List<ExpectedWaveformMessage> expectedWaveformMessages,
                List<WaveformLowFreqMessage> msgs) {
            for (int i = 0; i < expectedWaveformMessages.size(); i++) {
                expectedWaveformMessages.get(i).assertIsEqual(msgs.get(i));
            }
        }

    }

    @Test
    void settings1aMessage() throws IOException, URISyntaxException, Hl7ParseException {
        String hl7String = readHl7FromResource("hl7/settings1a.hl7");
        String expectedSourceLocation = "UCHT03ICUBED11";
        String expectedMappedLocation = "T03^T03 BY01^BY01-11";
        List<ExpectedWaveformMessage> expectedWaveformMessages = List.of(
                new ExpectedWaveformMessage("584", "11", "unitless", null, "Pressure Support / CPAP (PS)"),
                // must be able to accept the same variable with different units in the same message; this happens in practice
                new ExpectedWaveformMessage("1408", "0.13", "secs", 0.13, null),
                new ExpectedWaveformMessage("1408", "5", "%", 5.0, null),
                new ExpectedWaveformMessage("1332", "8", "cmH2O", 8.0, null),
                new ExpectedWaveformMessage("2104", "4", "cmH2O", 4.0, null),
                new ExpectedWaveformMessage("9114", "50", "%", 50.0, null),
                new ExpectedWaveformMessage("7878", "0.11", "secs", 0.11, null),
                new ExpectedWaveformMessage("2583", "1:2", "unitless", null, "1:2")
        );
        List<WaveformLowFreqMessage> actualMsgs = makeMessagesAndBasicChecks(
                hl7String, expectedSourceLocation, expectedMappedLocation, expectedWaveformMessages.size())
                .stream().map(m -> (WaveformLowFreqMessage)m).toList();
        ExpectedWaveformMessage.checkAllMessages(expectedWaveformMessages, actualMsgs);

    }

    @Test
    void settings1bMessage() throws IOException, URISyntaxException, Hl7ParseException {
        String hl7String = readHl7FromResource("hl7/settings1b.hl7");
        String expectedSourceLocation = "UCHT03ICUBED11";
        String expectedMappedLocation = "T03^T03 BY01^BY01-11";
        List<ExpectedWaveformMessage> expectedWaveformMessages = List.of(
                new ExpectedWaveformMessage("2047", "3", "unitless", null, "Flow Trig"),
                new ExpectedWaveformMessage("635", "19.5", "%", 19.5, null),
                new ExpectedWaveformMessage("1314", "17.3", "/min", 17.3, null),
                new ExpectedWaveformMessage("1570", "0.8", "cmH2O", 0.8, null),
                new ExpectedWaveformMessage("22", "17.3", "/min", 17.3, null)
                );
        List<WaveformLowFreqMessage> actualMsgs = makeMessagesAndBasicChecks(
                hl7String, expectedSourceLocation, expectedMappedLocation, expectedWaveformMessages.size())
                .stream().map(m -> (WaveformLowFreqMessage)m).toList();
        ExpectedWaveformMessage.checkAllMessages(expectedWaveformMessages, actualMsgs);
    }


    @Test
    void messageWithMoreThanOneRepeat() throws IOException, URISyntaxException {
        String hl7String = readHl7FromResource("hl7/test1.hl7");
        String hl7WithReps = hl7String.replace("42.50^", "42.50~");
        Hl7ParseException e = assertThrows(Hl7ParseException.class, () -> hl7ParseAndQueue.parseHl7Fully(hl7ParseAndQueue.parseHl7Headers(hl7WithReps)));
        assertTrue(e.getMessage().contains("only be 1 repeat"));
    }

    @Test
    void messageWithConflictingLocation() throws IOException, URISyntaxException {
        String hl7String = readHl7FromResource("hl7/test1.hl7");
        String hl7WithReps = hl7String.replace("PV1||I|UCHT03ICURM08|", "PV1||I|UCHT03ICURM07|");

        Hl7ParseException e = assertThrows(Hl7ParseException.class, () -> hl7ParseAndQueue.parseHl7Fully(hl7ParseAndQueue.parseHl7Headers(hl7WithReps)));
        assertTrue(e.getMessage().contains("Unexpected location"));
    }

    @ParameterizedTest
    @ValueSource(strings =
            {
                    "MSH^",
                    "MSH|^~\\&^|DATACAPTOR||||20240731142108.741+0100||ORU^R01|44444444444449ab|P|2.3||||||UNICODE UTF-8|\r",
                    "MSH|^~\\&",
                    "MSH|^~&\\|",
            }
    )
    void messageWithUnusualSeparators(String hl7String) throws IOException, URISyntaxException {
        Hl7ParseException e = assertThrows(Hl7ParseException.class, () -> hl7ParseAndQueue.parseHl7Fully(hl7ParseAndQueue.parseHl7Headers(hl7String)));
        assertTrue(e.getMessage().contains("separators"));
    }

}
