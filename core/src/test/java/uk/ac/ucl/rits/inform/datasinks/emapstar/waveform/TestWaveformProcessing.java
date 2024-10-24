package uk.ac.ucl.rits.inform.datasinks.emapstar.waveform;

import lombok.AllArgsConstructor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.jdbc.Sql;
import uk.ac.ucl.rits.inform.datasinks.emapstar.MessageProcessingBase;
import uk.ac.ucl.rits.inform.datasinks.emapstar.repos.visit_observations.VisitObservationAuditRepository;
import uk.ac.ucl.rits.inform.datasinks.emapstar.repos.visit_observations.VisitObservationTypeRepository;
import uk.ac.ucl.rits.inform.datasinks.emapstar.repos.visit_observations.WaveformRepository;
import uk.ac.ucl.rits.inform.informdb.visit_recordings.VisitObservationType;
import uk.ac.ucl.rits.inform.informdb.visit_recordings.Waveform;
import uk.ac.ucl.rits.inform.interchange.EmapOperationMessageProcessingException;
import uk.ac.ucl.rits.inform.interchange.visit_observations.WaveformMessage;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestWaveformProcessing extends MessageProcessingBase {
    @Autowired
    private WaveformRepository waveformRepository;
    @Autowired
    private VisitObservationAuditRepository visitObservationAuditRepository;
    @Autowired
    private VisitObservationTypeRepository visitObservationTypeRepository;

    @BeforeEach
    void setup() throws IOException {
    }

    @AllArgsConstructor
    class TestData {
        String sourceStreamId;
        String mappedStreamName;
        int numSamples;
        int samplingRate;
        int maxSamplesPerMessage;
        String sourceLocation;
        String mappedLocation;
        Instant obsDatetime;
        public String unit;
        public Instant getEndObsTime() {
            return obsDatetime.plus(numSamples * 1000_000L / samplingRate, ChronoUnit.MICROS);
        }
        Long expectedLocationVisitId;
    }

    @Test
    @Sql("/populate_db.sql")
    void testAddWaveform() throws EmapOperationMessageProcessingException {
        var allTests = new TestData[]{
                // Intended to be two patients each connected to two machines, but the nature of the
                // bed/machine IDs may not quite be like this.
                new TestData( "23", "stream 23", 20_000, 300, 900,
                        "source1", "T11E^T11E BY02^BY02-25", Instant.parse("2010-09-10T12:00:00Z"), "stream23unit", 106001L),
                new TestData( "24", "stream 24", 25_000, 50, 500,
                        "source2", "T42E^T42E BY03^BY03-17", Instant.parse("2010-09-14T15:27:00Z"), "stream24unit", 106002L),
                // matches location but not time
                new TestData( "23", "stream 23", 15_000, 300, 900,
                        "source1", "T11E^T11E BY02^BY02-25", Instant.parse("2010-09-14T16:00:00Z"), "stream23unit", null),
                // matches time but not location
                new TestData( "23", "stream 23", 17_000, 50, 500,
                        "source2", "T42E^T42E BY03^BY03-17", Instant.parse("2010-09-10T12:00:00Z"), "stream23unit", null),
                // unknown location
                new TestData( "23", "stream 23", 17_000, 50, 500,
                        "unknownlocation", null, Instant.parse("2010-09-10T12:00:00Z"), "stream23unit", null)
        };
        List<WaveformMessage> allMessages = new ArrayList<>();
        for (var test: allTests) {
            allMessages.addAll(
                    messageFactory.getWaveformMsgs(test.sourceStreamId, test.mappedStreamName,
                            test.samplingRate, test.numSamples, test.maxSamplesPerMessage, test.sourceLocation,
                            test.mappedLocation, test.obsDatetime, test.unit, null));
        }

        // must cope with messages in any order! Fixed seed to aid in debugging.
        Collections.shuffle(allMessages, new Random(42));

        for (WaveformMessage msg : allMessages) {
            processSingleMessage(msg);
        }

        int totalObservedNumSamples = 0;
        for (var test: allTests) {
            List<Waveform> waveformRows = filterByDatetimeInterval(
                    waveformRepository.findAllBySourceLocationOrderByObservationDatetime(test.sourceLocation),
                    test.obsDatetime,
                    test.getEndObsTime());

            assertFalse(waveformRows.isEmpty());
            // make sure we're testing the difficult case of multiple waveform rows that need to be stitched together
            assertTrue(waveformRows.size() > 1);
            Optional<Integer> observedNumSamples = waveformRows.stream().map(w -> w.getValuesArray().length).reduce(Integer::sum);
            assertEquals(test.numSamples, observedNumSamples.orElseThrow());
            totalObservedNumSamples += observedNumSamples.get();

            checkVisitObservationTypes(waveformRows, test.sourceStreamId, test.mappedStreamName);

            /* If we expect the data to be linkable to an existing hospital visit,
             * search by that visit's (hl7adt) location and see that we get the same result.
             * If not, it should be empty.
             * Is this repo query even useful? Might want to add time interval to it.
             */
            List<Waveform> byHl7AdtLocation = filterByDatetimeInterval(
                    waveformRepository.findAllByLocationOrderByObservationDatetime(test.mappedLocation),
                    test.obsDatetime,
                    test.getEndObsTime());

            if (test.expectedLocationVisitId == null) {
                assertTrue(waveformRows.stream().allMatch(aw -> aw.getLocationVisitId() == null));
                assertTrue(byHl7AdtLocation.isEmpty());
            } else {
                assertTrue(waveformRows.stream().allMatch(aw -> aw.getLocationVisitId().getLocationVisitId() == test.expectedLocationVisitId));

                // .equals does not work properly for our Entities, but we only need to check that it's
                // the same rows, so just check PKs
                Set<Long> idsByHl7Adt = byHl7AdtLocation.stream().map(Waveform::getWaveformId).collect(Collectors.toSet());
                Set<Long> idsBySource = waveformRows.stream().map(Waveform::getWaveformId).collect(Collectors.toSet());
                assertEquals(idsByHl7Adt, idsBySource);
            }
            List<Double> actualDataPointsAtLocation = new ArrayList<>();
            for (var row: waveformRows) {
                assertTrue(row.getValuesArray().length <= test.maxSamplesPerMessage);
                assertEquals(test.unit, row.getUnit());
                actualDataPointsAtLocation.addAll(Arrays.asList(row.getValuesArray()));
            }
            checkLooksLikeSineWave(actualDataPointsAtLocation);

            Instant projectedEndTime = null;
            for (var row : waveformRows) {
                Instant thisStartTime = row.getObservationDatetime();
                if (projectedEndTime != null) {
                    // rows should neatly abut
                    assertEquals(thisStartTime, projectedEndTime);
                }
                // the final point in the array nominally becomes invalid (1 / samplingRate)
                // seconds after its start time
                projectedEndTime = thisStartTime.plus(
                        row.getValuesArray().length * 1000_000 / row.getSamplingRate(),
                        ChronoUnit.MICROS);
            }
            long totalExpectedTimeMicros = 1_000_000L * test.numSamples / test.samplingRate;
            long totalActualTimeMicros = waveformRows.get(0).getObservationDatetime().until(projectedEndTime, ChronoUnit.MICROS);
            assertEquals(totalExpectedTimeMicros, totalActualTimeMicros);
        }
        assertEquals(Arrays.stream(allTests).map(d -> d.numSamples).reduce(Integer::sum).get(), totalObservedNumSamples);
        Map<String, VisitObservationType> allWaveformVO =
                visitObservationTypeRepository.findAllBySourceObservationType("waveform")
                        .stream().collect(Collectors.toMap(VisitObservationType::getIdInApplication, Function.identity()));
        assertEquals(2, allWaveformVO.size());

        for (String id: List.of("23", "24")) {
            VisitObservationType vot = allWaveformVO.get(id);
            assertEquals(id, vot.getInterfaceId());
            assertEquals(id, vot.getIdInApplication());
            assertEquals("stream " + id, vot.getName());
            assertTrue(vot.getIsRealTime());
        }
    }

    /**
     * Does the list of points look roughly like a sine wave?
     * A triangular wave would probably pass too, but the most likely error is that
     * data would be missing or out of order.
     */
    private void checkLooksLikeSineWave(List<Double> actualDataPointsAtLocation) {
        List<Integer> crossesZero = new ArrayList<>();
        List<Integer> inflectionPoints = new ArrayList<>();
        Double max = actualDataPointsAtLocation.stream().max(Double::compare).get();
        Double min = actualDataPointsAtLocation.stream().min(Double::compare).get();
        assertTrue(approxEqualsAbs(max, 1.0, 0.0001));
        assertTrue(approxEqualsAbs(min, -1.0, 0.0001));
        Double previousGradient = null;
        Double maxGradient = Double.MIN_VALUE;
        Double minGradient = Double.MAX_VALUE;
        for (int i = 1; i < actualDataPointsAtLocation.size(); i++) {
            Double thisValue = actualDataPointsAtLocation.get(i);
            Double previousValue = actualDataPointsAtLocation.get(i - 1);
            Double thisGradient = thisValue - previousValue; // ignore x scale
            if (thisGradient > maxGradient) {
                maxGradient = thisGradient;
            }
            if (thisGradient < minGradient) {
                minGradient = thisGradient;
            }
            if (thisValue.compareTo(0.0) != previousValue.compareTo(0.0)) {
                crossesZero.add(i);
                // At zero crossings, gradient should be at its max (or min).
                // Wait for an entire cycle to have passed so the min and max gradients are correct.
                if (crossesZero.size() >= 3) {
                    approxEqualsAbs(Math.abs(thisGradient), maxGradient, maxGradient * 0.01);
                    approxEqualsAbs(maxGradient, -minGradient, maxGradient * 0.01);
                }
            }
            if (previousGradient != null && thisGradient.compareTo(0.0) != previousGradient.compareTo(0.0)) {
                inflectionPoints.add(i);
                // at inflection points, gradient should be 0
                approxEqualsAbs(thisGradient, 0, 0.01);
            }
            // sine wave is never flat
            assertNotEquals(thisValue, previousValue);

            previousGradient = thisGradient;
        }

        // Check that inflection points and zero crossings are individually evenly spaced,
        // and that zero crossings are evenly spaced from inflection points
        List<Integer> gapsBetweenInfl = calcGapsBetweenValues(inflectionPoints);
        List<Integer> gapsBetweenCrossings = calcGapsBetweenValues(crossesZero);
        Double gapsInflMean = checkAllValuesRoughlyEqual(gapsBetweenInfl);
        Double gapsCrosMean = checkAllValuesRoughlyEqual(gapsBetweenCrossings);
        assertTrue(approxEqualsAbs(gapsCrosMean, gapsInflMean, gapsCrosMean * 0.01));
        List<Integer> inflAndCrossings = new ArrayList<>();
        inflAndCrossings.addAll(inflectionPoints);
        inflAndCrossings.addAll(crossesZero);
        inflAndCrossings.sort(Integer::compare);
        List<Integer> gapsBetweenInflAndCrossings = calcGapsBetweenValues(inflAndCrossings);
        double allPointsMean = checkAllValuesRoughlyEqual(gapsBetweenInflAndCrossings);
        assertTrue(approxEqualsAbs(allPointsMean, gapsCrosMean / 2, allPointsMean * 0.01));
    }

    private static double checkAllValuesRoughlyEqual(List<Integer> list) {
        double mean = list.stream().reduce(Integer::sum).get() / (double) list.size();
        //System.out.println("checkAllValuesRoughlyEqual: mean = " + mean + ", list = " + list);
        assertTrue(list.stream().allMatch(g -> approxEqualsAbs(g, mean, mean * 0.01)));
        return mean;
    }

    private static List<Integer> calcGapsBetweenValues(List<Integer> list) {
        List<Integer> gaps = new ArrayList<>();
        for (int i = 1; i < list.size(); i++) {
            var thisValue = list.get(i);
            var previousValue = list.get(i - 1);
            gaps.add(thisValue - previousValue);
        }
        return gaps;
    }

    private static boolean approxEqualsAbs(double d1, double d2, double absTol) {
        return Math.abs(d1 - d2) <= Math.abs(absTol);
    }

    private static void checkVisitObservationTypes(List<Waveform> waveformRows,
                                                   String sourceStreamId, String mappedStreamName) {
        // visit observations should all be the same, and be the right thing
        List<Long> distinctVisitObservationIds =
                waveformRows.stream()
                        .map(Waveform::getVisitObservationTypeId)
                        .map(VisitObservationType::getVisitObservationTypeId)
                        .distinct().toList();
        assertEquals(distinctVisitObservationIds.size(), 1);
        VisitObservationType identicalVisitObs = waveformRows.get(0).getVisitObservationTypeId();
        assertEquals(sourceStreamId, identicalVisitObs.getIdInApplication());
        assertEquals(mappedStreamName, identicalVisitObs.getName());
    }

    private static List<Waveform> filterByDatetimeInterval(Iterable<Waveform> waveforms, Instant beginTime, Instant endTime) {
        List<Waveform> filteredRows = new ArrayList<>();
        for (var waveform: waveforms) {
            if (waveform.getObservationDatetime().compareTo(beginTime) >= 0
                    && waveform.getObservationDatetime().compareTo(endTime) < 0) {
                filteredRows.add(waveform);
            }
        }
        return filteredRows;
    }

}
