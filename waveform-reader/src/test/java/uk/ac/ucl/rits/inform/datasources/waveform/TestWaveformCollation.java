package uk.ac.ucl.rits.inform.datasources.waveform;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import uk.ac.ucl.rits.inform.interchange.test.helpers.InterchangeMessageFactory;
import uk.ac.ucl.rits.inform.interchange.visit_observations.WaveformMessage;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringJUnitConfig
@SpringBootTest
@ActiveProfiles("test")
public class TestWaveformCollation {
    @Autowired
    private Hl7ParseAndSend hl7ParseAndSend;
    @Autowired
    private WaveformCollator waveformCollator;

    private InterchangeMessageFactory messageFactory = new InterchangeMessageFactory();

    Instant messageStartDatetime = Instant.parse("2022-03-04T12:11:00Z");

    @BeforeEach
    void clearMessages() {
        waveformCollator.pendingMessages.clear();
    }

    List<WaveformMessage> makeTestMessages() {
        // Check that we can handle adding messages from different streams,
        // as would be found in a real HL7 message
        List<WaveformMessage> uncollatedMsgs = messageFactory.getWaveformMsgs(
                "59912", "something1",
                300, 3000, 5, "UCHT03TEST",
                "", messageStartDatetime, "unit1", ChronoUnit.MILLIS);
        List<WaveformMessage> uncollatedMsgs2 = messageFactory.getWaveformMsgs(
                "59913", "something2",
                300, 3000, 5, "UCHT03TEST",
                "",
                messageStartDatetime, //.plus(5500, ChronoUnit.MILLIS),
                "unit2", // XXX: unit should be part of key??
                ChronoUnit.MILLIS);
        uncollatedMsgs.addAll(uncollatedMsgs2);
        assertEquals(1200, uncollatedMsgs.size());
        return uncollatedMsgs;
    }

    // return the one that didn't get added, in case you want to add it later
    private WaveformMessage makeAndAddTestMessagesWithGap() throws WaveformCollator.CollationException {
        List<WaveformMessage> inputMessages = makeTestMessages();
        WaveformMessage removed = inputMessages.remove(300);
        // they must work in any order
        Collections.shuffle(inputMessages, new Random(42));
        waveformCollator.addMessages(inputMessages);
        return removed;
    }

    private void makeAndAddTestMessages() throws WaveformCollator.CollationException {
        List<WaveformMessage> inputMessages = makeTestMessages();
        // they must work in any order
        Collections.shuffle(inputMessages, new Random(42));
        waveformCollator.addMessages(inputMessages);
    }

    static Stream<Arguments> noGapsData() {
        // We are adjusting the target number of samples config option rather than
        // the actual number of samples supplied, which may be a bit unintuitive but
        // is easier and amounts to the same thing.
        return Stream.of(
                // only just happened
                Arguments.of(3000, 10000, List.of(3000), 0),
                Arguments.of(3001, 10000, List.of(), 600),
                Arguments.of(2995, 10000, List.of(2995), 1),
                Arguments.of(2996, 10000, List.of(2995), 1),
                Arguments.of(1400, 10000, List.of(1400, 1400), 40),
                // comfortably in past
                Arguments.of(3000, 25000, List.of(3000), 0),
                Arguments.of(3001, 25000, List.of(3000), 0),
                Arguments.of(2995, 25000, List.of(2995, 5), 0),
                Arguments.of(2996, 25000, List.of(2995, 5), 0),
                Arguments.of(1400, 25000, List.of(1400, 1400, 200), 0)
        );
    }

    /**
     * Test with no gaps in the source data, but will still be broken into multiple messages due to sample limit.
     *
     * @param targetNumSamples the limit for splitting messages
     * @param nowAfterFirstMessageMillis when to perform the test (the "now" time), expressed in millis after the
     *                                   observation time of the first message
     * @param expectedNewMessageSampleCounts number of elements defines expected number of messages, and the value is
     *                                       the number of samples each returned message is expected to have
     * @param expectedRemainingMessages how many messages (not samples) are expected to remain uncollated
     */
    @ParameterizedTest
    @MethodSource("noGapsData")
    void noGaps(
            int targetNumSamples,
            int nowAfterFirstMessageMillis,
            List<Integer> expectedNewMessageSampleCounts,
            int expectedRemainingMessages) throws WaveformCollator.CollationException {
        int waitForDataLimitMillis = 15000;
        ChronoUnit assumedRounding = ChronoUnit.MILLIS;
        // GIVEN some uncollated messages (straight from HL7)
        makeAndAddTestMessages();
        Pair<String, String> keyOfInterest = new ImmutablePair<>("UCHT03TEST", "59912");
        assertEquals(2, waveformCollator.pendingMessages.size());
        assertEquals(600, waveformCollator.pendingMessages.get(keyOfInterest).size());

        // WHEN I collate the messages (which may be comfortably in the past, or have only just happened)
        Instant now = messageStartDatetime.plus(nowAfterFirstMessageMillis, assumedRounding);
        List<WaveformMessage> allCollatedMsgs = waveformCollator.getReadyMessages(
                now, targetNumSamples, waitForDataLimitMillis, assumedRounding);
        // only test messages from one stream
        List<WaveformMessage> collatedMsgs =
                allCollatedMsgs.stream().filter(msg -> waveformCollator.makeKey(msg).equals(keyOfInterest)).toList();

        // THEN the messages have been combined into much fewer messages and the pending list is smaller or empty
        assertEquals(expectedNewMessageSampleCounts.size(), collatedMsgs.size());
        List<Integer> actualSampleCounts = collatedMsgs.stream().map(m -> m.getNumericValues().get().size()).toList();
        assertEquals(expectedNewMessageSampleCounts, actualSampleCounts);
        assertEquals(expectedRemainingMessages, waveformCollator.pendingMessages.get(keyOfInterest).size());

        // getting again doesn't get any more messages
        List<WaveformMessage> collatedMsgsRepeat = waveformCollator.getReadyMessages(
                now, targetNumSamples, waitForDataLimitMillis, assumedRounding);
        assertEquals(0, collatedMsgsRepeat.size());
    }

    static Stream<Arguments> gapInMessagesData() {
        return Stream.of(
                // > vs >= is not a big deal

                // Not enough time has passed so no collation at first. Missing message returns before any collation,
                // so we get all 3000 in one message on the second attempt.
                Arguments.of(19999, List.of(), List.of(3000)),
                // One message got collated initially. Missing message returns and is collated with the second chunk.
                Arguments.of(20001, List.of(1500), List.of(1500)),
                Arguments.of(24999, List.of(1500), List.of(1500)),
                // Both messages got collated initially. When the missing message returns there is no data left so
                // it's "collated" by itself.
                Arguments.of(25001, List.of(1500, 1495), List.of(5))
        );
    }
    @ParameterizedTest
    @MethodSource("gapInMessagesData")
    void gapInMessages(
            int millisAfter,
            List<Integer> expectedSampleSizes,
            List<Integer> expectedSampleSizesAfterLateMessage) throws WaveformCollator.CollationException {
        int waitForDataLimitMillis = 15000;
        int targetCollatedMessageSamples = 3000;
        WaveformMessage removedMessage = makeAndAddTestMessagesWithGap();
        // We started with ~10 seconds of data, with a gap halfway. The default wait limit is 15 seconds after the gap,
        // which is therefore 20 seconds after the first set of data, and 25 seconds after the second set.
        Instant now = messageStartDatetime.plus(millisAfter, ChronoUnit.MILLIS);
        List<WaveformMessage> allCollatedMsgs = waveformCollator.getReadyMessages(
                now, targetCollatedMessageSamples, waitForDataLimitMillis, ChronoUnit.MILLIS);
        Pair<String, String> keyOfInterest = new ImmutablePair<>("UCHT03TEST", "59912");
        // only test messages from one stream
        List<WaveformMessage> collatedMsgs =
                allCollatedMsgs.stream().filter(msg -> waveformCollator.makeKey(msg).equals(keyOfInterest)).toList();

        /* The gap means that instead of a solid chunk of 3000 samples of data (600 messages),
         * there is one chunk of 1500 samples and one of 1495.
         * If not enough time has passed, we will collate nothing.
         * Even if enough time has passed that we will allow the message size to be under the usual threshold,
         * we still can't straddle the gap within a single message, so make two messages of 1500 + 1495, or
         * one of 1500 if only a moderate amount of time has passed.
         */
        assertEquals(expectedSampleSizes.size(), collatedMsgs.size());
        List<Integer> actualSampleSizes = collatedMsgs.stream().map(m -> m.getNumericValues().get().size()).toList();
        assertEquals(expectedSampleSizes, actualSampleSizes);

        // The missing message has now turned up!
        waveformCollator.addMessages(List.of(removedMessage));

        // Sufficiently far in the future, get messages again and see that collation happens where possible
        Instant now2 = now.plus(waitForDataLimitMillis, ChronoUnit.MILLIS);
        List<WaveformMessage> secondBatchMessages = waveformCollator.getReadyMessages(
                now2, targetCollatedMessageSamples, waitForDataLimitMillis, ChronoUnit.MILLIS);
        List<Integer> actualSampleSizes2 = secondBatchMessages.stream().map(m -> m.getNumericValues().get().size()).toList();
        assertEquals(expectedSampleSizesAfterLateMessage.size(), secondBatchMessages.size());
        assertEquals(expectedSampleSizesAfterLateMessage, actualSampleSizes2);
        assertEquals(0, waveformCollator.pendingMessages.get(keyOfInterest).size());
    }

}
