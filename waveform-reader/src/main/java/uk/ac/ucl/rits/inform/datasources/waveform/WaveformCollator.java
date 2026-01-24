package uk.ac.ucl.rits.inform.datasources.waveform;

import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.commons.lang3.tuple.Triple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import uk.ac.ucl.rits.inform.interchange.InterchangeValue;
import uk.ac.ucl.rits.inform.interchange.visit_observations.WaveformMessage;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

@Component
/**
 * Read interchange messages produced by {@link Hl7ParseAndQueue}, identify
 * contiguous data to turn them into bigger interchange messages for greater
 * DB storage efficiency.
 */
public class WaveformCollator {
    private final Logger logger = LoggerFactory.getLogger(WaveformCollator.class);
    protected final Map<Triple<String, String, String>, SortedMap<Instant, WaveformMessage>> pendingMessages = new HashMap<>();

    Triple<String, String, String> makeKey(WaveformMessage msg) {
        return new ImmutableTriple<>(msg.getSourceLocationString(), msg.getSourceVariableId(), msg.getSourceChannelId());
    }

    /**
     * Add short messages from the same patient for collating.
     * @param messagesToAdd messages to add, can be for different location+stream
     * @throws CollationException if a message duplicates another message
     */
    public void addMessages(List<WaveformMessage> messagesToAdd) throws CollationException {
        Map<Triple<String, String, String>, List<WaveformMessage>> messagesToAddByKey = new HashMap<>();
        for (WaveformMessage toAdd: messagesToAdd) {
            Triple<String, String, String> key = makeKey(toAdd);
            messagesToAddByKey.computeIfAbsent(key, k -> new ArrayList<>()).add(toAdd);
        }

        /* Lock the entire structure as briefly as possible, only to add new entries that don't exist.
         * Lock because we mustn't simultaneously iterate over it in the reader thread.
         */
        synchronized (pendingMessages) {
            for (var key: messagesToAddByKey.keySet()) {
                pendingMessages.computeIfAbsent(key, k -> new TreeMap<>());
            }
        }

        /* The bulk of time is spent only with a lock held on the data structure specific to
         * the location+stream, thus enabling more parallelism.
         * Need lock because we may be trying to collate at the same time as we're adding here.
         * Group together all the messages for a particular location+stream, so that the lock
         * for each one only needs to be taken out once.
         */
        for (var key: messagesToAddByKey.keySet()) {
            SortedMap<Instant, WaveformMessage> existingMessages = pendingMessages.get(key);
            synchronized (existingMessages) {
                for (WaveformMessage msg: messagesToAddByKey.get(key)) {
                    Instant observationTime = msg.getObservationTime();
                    // messages may arrive out of order, but TreeMap will keep them sorted by obs time
                    WaveformMessage existing = existingMessages.put(observationTime, msg);
                    if (existing != null) {
                        // in future we may want to compare them and only log error if they differ
                        throw new CollationException(String.format("Already existing message with time %s: %s",
                                observationTime, existing));
                    }
                }
            }
        }
    }

    /**
     * If a sufficient run of gapless messages exists for a patient, collate them
     * and delete the source messages.
     * @param nowTime for the purposes of determining how old the data is, this is the "now" time.
     *                Should be set to Instant.now() in production, but can be set differently for testing.
     * @param targetCollatedMessageSamples Wait for this many samples to exist in the queue before collating into
     *                                     a message. If we've waited more than waitForDataLimitMillis, then collate
     *                                     even if fewer samples are present. Never exceed this target.
     * @param waitForDataLimitMillis Time limit for when to relax the requirement to reach
     *                               targetCollatedMessageSamples before collating into a message.
     * @param assumedRounding what level of rounding to assume has been applied to the message timestamps, or null to
     *                        not make such an assumption.
     * @return the collated messages that are now ready for sending, may be empty if none are ready
     * @throws CollationException if any set within pendingMessages contains messages not in
     *                             fact all from the same patient+stream
     */
    public List<WaveformMessage> getReadyMessages(Instant nowTime,
                                                  int targetCollatedMessageSamples,
                                                  int waitForDataLimitMillis,
                                                  ChronoUnit assumedRounding) throws CollationException {
        List<WaveformMessage> newMessages = new ArrayList<>();
        logger.info("Pending messages: {} - {} location+stream combos (of which {} non-empty)",
                getPendingMessageCount(),
                pendingMessages.size(),
                pendingMessages.values().stream().filter(pm -> !pm.isEmpty()).count());
        logger.debug("Pending total samples: {}", getPendingSampleCount());
        List<SortedMap<Instant, WaveformMessage>> pendingMessagesSnapshot;
        synchronized (pendingMessages) {
            // Here we (briefly) iterate over pendingMessages, so take out a lock to prevent
            // undefined behaviour should we happen to be simultaneously adding items to this map.
            // (Note that items are never deleted)
            pendingMessagesSnapshot = new ArrayList<>(pendingMessages.values());
        }
        // The snapshot may become slightly out of date, but that's fine because any new
        // entries will get handled next time. The advantage is to allow
        // more fine-grained locking to take place here.
        for (SortedMap<Instant, WaveformMessage> perPatientMap: pendingMessagesSnapshot) {
            while (true) {
                // There can be zero to multiple chunks that need turning into messages
                WaveformMessage newMsg;
                synchronized (perPatientMap) {
                    newMsg = collateContiguousData(perPatientMap, nowTime,
                            targetCollatedMessageSamples, waitForDataLimitMillis, assumedRounding);
                }
                if (newMsg == null) {
                    break;
                } else {
                    newMessages.add(newMsg);
                }
            }
        }
        return newMessages;
    }


    /**
     * Given a sorted map of messages (all for same patient+stream), squash as much as possible
     * into a single message, respecting the target number of samples. If a time gap is detected
     * in the sequence of messages, stop. Ie. do not straddle the gap within the same message.
     * Remove messages from the structure which were used as source data for the collated message.
     * Returns only one message, must be called repeatedly to see if more collating can be done.
     * @param perPatientMap sorted messages to collate, will have source items deleted from it
     * @param nowTime see {@link #getReadyMessages}
     * @param targetCollatedMessageSamples see {@link #getReadyMessages}
     * @param waitForDataLimitMillis see {@link #getReadyMessages}
     * @param assumedRounding see {@link #getReadyMessages}
     * @return the collated message, or null if the messages cannot be collated
     * @throws CollationException if perPatientMap messages are not in fact all from the same patient+stream
     */

    private WaveformMessage collateContiguousData(SortedMap<Instant, WaveformMessage> perPatientMap,
                                                  Instant nowTime,
                                                  int targetCollatedMessageSamples,
                                                  int waitForDataLimitMillis,
                                                  ChronoUnit assumedRounding) throws CollationException {
        if (perPatientMap.isEmpty()) {
            // maps are not removed after being emptied, so this situation can exist and is harmless
            return null;
        }
        WaveformMessage firstMsg = perPatientMap.get(perPatientMap.firstKey());
        Triple<String, String, String> firstKey = makeKey(firstMsg);

        int sizeBefore = perPatientMap.size();
        long sampleCount = 0;
        WaveformMessage previousMsg = null;
        // existing values are not necessarily in mutable lists so use a new ArrayList
        List<Double> newNumericValues = new ArrayList<>();
        Iterator<Map.Entry<Instant, WaveformMessage>> perPatientMapIter = perPatientMap.entrySet().iterator();
        // keep track of incoming message sizes for general interest (does not affect collation algorithm)
        Map<Integer, Integer> uncollatedMessageSizes = new HashMap<>();
        int messagesToCollate = 0;
        while (perPatientMapIter.hasNext()) {
            Map.Entry<Instant, WaveformMessage> entry = perPatientMapIter.next();
            WaveformMessage msg = entry.getValue();
            Triple<String, String, String> thisKey = makeKey(msg);
            if (!thisKey.equals(firstKey)) {
                throw new CollationException(String.format("Key Mismatch: %s vs %s", firstKey, thisKey));
            }

            int thisMessageSampleCount = msg.getNumericValues().get().size();
            int messageSizeCount = uncollatedMessageSizes.getOrDefault(thisMessageSampleCount, 0);
            uncollatedMessageSizes.put(thisMessageSampleCount, messageSizeCount + 1);

            // Ideally this code would be reworked, but for now it's important for sampleCount to meet or exceed
            // targetCollatedMessageSamples even if we break out here (and thus don't include the current message).
            // This is needed to prevent the check later on that would say the total sample count isn't big enough
            // to make a message unless the data is old enough that we should collate regardless.
            // Essentially, the later code is failing to realise that you can "meet" the target even if you're slightly
            // short of it, because the next message would take us over the target.
            sampleCount += thisMessageSampleCount;
            if (sampleCount > targetCollatedMessageSamples) {
                logger.debug("Reached sample target ({} > {}), collated message size {}, collated message span: {} -> {}",
                        sampleCount, targetCollatedMessageSamples, sampleCount - thisMessageSampleCount,
                        firstMsg.getObservationTime(), msg.getObservationTime());
                break;
            }

            if (previousMsg != null) {
                Instant expectedNextDatetime = previousMsg.getExpectedNextObservationDatetime();
                try {
                    Instant gapUpperBound = checkGap(msg, expectedNextDatetime, assumedRounding);
                    if (gapUpperBound != null) {
                        logger.info("Key {} ({}Hz), collated message span: {} -> {} ({} milliseconds, {} messages, {} samples)",
                                makeKey(msg),
                                msg.getSamplingRate(),
                                firstMsg.getObservationTime(),
                                expectedNextDatetime,
                                firstMsg.getObservationTime().until(expectedNextDatetime, ChronoUnit.MILLIS),
                                messagesToCollate,
                                sampleCount);
                        // Found a gap, stop here, excluding `msg`.
                        // Collation may still happen if data is old enough that we don't want to wait for more.
                        break;
                    }
                } catch (CollationOverlapException coe) {
                    logger.error("""
                                    Key {} ({}Hz), {}, between:
                                     previous message ({} -> {}) {} samples
                                     this message     ({} -> {}) {} samples
                                     """,
                            makeKey(msg),
                            msg.getSamplingRate(),
                            coe.getMessage(),
                            previousMsg.getObservationTime(), expectedNextDatetime,
                            previousMsg.getNumericValues().get().size(),
                            msg.getObservationTime(), msg.getExpectedNextObservationDatetime(),
                            msg.getNumericValues().get().size());
                    // The data can't be corrected, but we can at least stop collating at this point.
                    // The overlapping message will be the first message of the next collation run,
                    // which at least exposes the overlap in the database rather than trying to obscure it.
                    break;
                }
            }
            previousMsg = msg;

            // don't modify yet, because we don't yet know if we will reach criteria to collate (num samples, time passed)
            messagesToCollate++;
        }

        // If we have not reached the message size threshold, whether because there aren't enough samples
        // or we reached a gap, then do not collate yet; give the data a bit more time to appear.
        // UNLESS enough time has already passed, then prioritise timeliness and collate anyway.
        // (If the data does subsequently arrive, then it'll be collated into a different message)
        // In other words, if not enough samples and not enough time has passed, then do not collate.
        Instant expectedNextDatetime = previousMsg.getExpectedNextObservationDatetime();
        if (sampleCount < targetCollatedMessageSamples
                && expectedNextDatetime.until(nowTime, ChronoUnit.MILLIS) <= waitForDataLimitMillis) {
            return null;
        }

        logger.info("Collating {} messages into one. Total samples {}. Source messages contained sample counts: {}",
                messagesToCollate, sampleCount, uncollatedMessageSizes);

        // Do the actual collation now that we know how far to go.
        Iterator<Map.Entry<Instant, WaveformMessage>> secondPassIter = perPatientMap.entrySet().iterator();
        for (int i = 0; i < messagesToCollate; i++) {
            Map.Entry<Instant, WaveformMessage> entry = secondPassIter.next();
            WaveformMessage msg = entry.getValue();
            newNumericValues.addAll(msg.getNumericValues().get());
            // Remove all messages from the map that are used as source data, even the first one.
            // The underlying message object of the first element will still exist.
            secondPassIter.remove();
        }
        firstMsg.setNumericValues(new InterchangeValue<>(newNumericValues));
        int sizeAfter = perPatientMap.size();
        logger.info("Key {}, Collated {} messages into one, ({} data points)",
                firstKey, sizeBefore - sizeAfter, sampleCount);
        return firstMsg;
    }

    private Instant checkGap(WaveformMessage msg, Instant expectedNextDatetime, ChronoUnit assumedRounding) throws CollationOverlapException {
        // gap between this message and previous message
        long gapSizeMicros = expectedNextDatetime.until(msg.getObservationTime(), ChronoUnit.MICROS);
        /* The timestamps in the messages will be rounded. Not sure if they round down or round to nearest.
         * Take 3.33 ms as an example, a common sampling period (300Hz): rounding to the nearest ms
         * can produce a large relative error, but never more than a millisecond.
         * So, if it has been rounded to the millisecond, allow it to be one millisecond out, and so on.
         */
        logger.trace("expectedNextDatetime {}, msg.getObservationTime() {}",
                expectedNextDatetime, msg.getObservationTime());

        long allowedGapMicros = assumedRounding.getDuration().toNanos() / 1000;
        // Distinguish between gaps (positive difference) and overlap (negative difference).
        // Overlap is a sign that the actual sampling rate is inconsistent with the metadata!
        boolean gapTooBig = gapSizeMicros > allowedGapMicros;
        boolean overlapTooBig = gapSizeMicros < -allowedGapMicros;
        if (gapTooBig) {
            // We expect this to happen occasionally - it's not an error.
            logger.info("Key {}, Gap too big ({} vs unrounded)", makeKey(msg), gapSizeMicros);
            return msg.getObservationTime();
        }
        if (overlapTooBig) {
            // Overlap is an error that can't really be recovered from.
            // Would need to investigate whether the sampling rate is different to what we expected based on
            // metadata.
            throw new CollationOverlapException(String.format("OVERLAP of %d µs", gapSizeMicros));
        }

        return null;
    }

    /**
     * @return The total number of samples pending in the queue.
     */
    public int getPendingSampleCount() {
        // Iteration, even without modification, can't be done while the structure might be being modified.
        synchronized (pendingMessages) {
            int totalSamples = 0;
            for (var perPatientStream: pendingMessages.values()) {
                synchronized (perPatientStream) {
                    for (var msgs: perPatientStream.values()) {
                        totalSamples += msgs.getNumericValues().get().size();
                    }
                }
            }
            return totalSamples;
        }
    }

    /**
     * @return The number of messages pending (uncollated) in the queue.
     */
    public int getPendingMessageCount() {
        synchronized (pendingMessages) {
            return pendingMessages.values().stream().map(Map::size).reduce(Integer::sum).orElse(0);
        }
    }

    class CollationException extends Throwable {
        CollationException(String format) {
        }
    }
    class CollationOverlapException extends CollationException {
        CollationOverlapException(String format) {
            super(format);
        }
    }
}
