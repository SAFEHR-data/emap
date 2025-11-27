package uk.ac.ucl.rits.inform.datasources.waveform;

import lombok.Getter;
import lombok.Setter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import uk.ac.ucl.rits.inform.datasources.waveform.hl7parse.Hl7Message;
import uk.ac.ucl.rits.inform.datasources.waveform.hl7parse.Hl7ParseException;
import uk.ac.ucl.rits.inform.datasources.waveform.hl7parse.Hl7Segment;
import uk.ac.ucl.rits.inform.interchange.InterchangeValue;
import uk.ac.ucl.rits.inform.interchange.visit_observations.WaveformMessage;

import java.io.IOException;
import java.time.DateTimeException;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAccessor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Receive HL7 messages, transform each to an interchange message, and
 * store them in memory ready for collation into bigger interchange messages
 * (see {@link WaveformCollator}).
 */
@Component
public class Hl7ParseAndQueue {
    private static final Logger logger = LoggerFactory.getLogger(Hl7ParseAndQueue.class);
    private final WaveformOperations waveformOperations;
    private final WaveformCollator waveformCollator;
    private final SourceMetadata sourceMetadata;
    private final LocationMapping locationMapping;
    private final Hl7MessageTimeSlotCalculator hl7MessageTimeSlotCalculator;
    private final Hl7MessageSaver hl7MessageSaver;
    private long numHl7 = 0;

    Hl7ParseAndQueue(WaveformOperations waveformOperations,
                     WaveformCollator waveformCollator,
                     SourceMetadata sourceMetadata,
                     LocationMapping locationMapping,
                     Hl7MessageTimeSlotCalculator hl7MessageTimeSlotCalculator,
                     Hl7MessageSaver hl7MessageSaver) {
        this.waveformOperations = waveformOperations;
        this.waveformCollator = waveformCollator;
        this.sourceMetadata = sourceMetadata;
        this.locationMapping = locationMapping;
        this.hl7MessageTimeSlotCalculator = hl7MessageTimeSlotCalculator;
        this.hl7MessageSaver = hl7MessageSaver;
    }

    public record PartiallyParsedMessage(
            String rawHl7Trimmed,
            Hl7Message hl7MessageParser, // to allow for continued parsing
            String bedLocation,
            Instant messageTimestamp,
            Instant messageTimeslot) {}

    public record FullyParsedMessage(
            String rawHl7Trimmed,
            List<WaveformMessage> waveformMessages,
            String bedLocation,
            Instant messageTimestamp,
            Instant messageTimeslot) {}

    /**
     * Parse just the timestamp and location so that the message can be routed appropriately.
     * Store the parser object so parsing can be resumed when ready.
     * @param messageAsStr HL7 message as a string
     * @return partial information, possibly
     * @throws Hl7ParseException if parsing failed
     */
    public PartiallyParsedMessage parseHl7Headers(String messageAsStr) throws Hl7ParseException {
        int origSize = messageAsStr.length();
        // messages are separated with vertical tabs and extra carriage returns, so remove
        messageAsStr = messageAsStr.strip();
        if (messageAsStr.isEmpty()) {
            // message was all whitespace, ignore
            logger.info("Ignoring empty or all-whitespace message");
            return new PartiallyParsedMessage(messageAsStr, null, null, null, null);
        }
        logger.debug("Parsing message of size {} ({} including stray whitespace)", messageAsStr.length(), origSize);
        Hl7Message message = new Hl7Message(messageAsStr);
        // Because each OBR could have its own timestamp, to obtain a timestamp for the whole message,
        // use MSH-7
        Instant messageHeaderTimestamp = null;
        try {
            messageHeaderTimestamp = interpretWaveformTimestamp(message.getField("MSH", 7));
        } catch (DateTimeException e) {
            throw (Hl7ParseException) new Hl7ParseException(messageAsStr, "Datetime parsing failed").initCause(e);
        }
        String pv1LocationId = message.getField("PV1", 3);
        String messageType = message.getField("MSH", 9);
        if (!messageType.equals("ORU^R01")) {
            throw new Hl7ParseException(messageAsStr, "Was expecting ORU^R01, got " + messageType);
        }
        return new PartiallyParsedMessage(messageAsStr,
                message,
                pv1LocationId,
                messageHeaderTimestamp,
                hl7MessageTimeSlotCalculator.truncateTime(messageHeaderTimestamp));
    }

    FullyParsedMessage parseHl7Fully(PartiallyParsedMessage partiallyParsedMessage) throws Hl7ParseException {
        Hl7Message message = partiallyParsedMessage.hl7MessageParser();
        List<WaveformMessage> allWaveformMessages = new ArrayList<>();
        if (message == null) {
            return new FullyParsedMessage(
                    partiallyParsedMessage.rawHl7Trimmed,
                    allWaveformMessages,
                    null,
                    null,
                    null);
        }
        String pv1LocationId = partiallyParsedMessage.bedLocation();
        List<Hl7Segment> allObr = message.getSegments("OBR");
        String messageIdBase = message.getField("MSH", 10);
        int obrI = 0;
        for (var obr: allObr) {
            obrI++;
            String locationId = obr.getField(10);
            List<Hl7Segment> allObx = obr.getChildSegments("OBX");
            int obxI = 0;
            for (var obx: allObx) {
                obxI++;
                String obsDatetimeStr = obx.getField(14);

                if (!pv1LocationId.equals(locationId)) {
                    throw new Hl7ParseException(partiallyParsedMessage.rawHl7Trimmed, "Unexpected location " + locationId + "|" + pv1LocationId);
                }

                Instant obsDatetime;
                try {
                    obsDatetime = interpretWaveformTimestamp(obsDatetimeStr);
                } catch (DateTimeException e) {
                    throw (Hl7ParseException) new Hl7ParseException(partiallyParsedMessage.rawHl7Trimmed, "Datetime parsing failed").initCause(e);
                }

                String streamId = obx.getField(3);

                Optional<SourceMetadataItem> metadataOpt = sourceMetadata.getStreamMetadata(streamId);
                if (metadataOpt.isEmpty()) {
                    logger.warn("Skipping stream {}, unrecognised streamID", streamId);
                    continue;
                }
                SourceMetadataItem metadata = metadataOpt.get();
                if (!metadata.isUsable()) {
                    logger.warn("Skipping stream {}, insufficient metadata", streamId);
                    continue;
                }
                // Sampling rate and stream description is not in the message, so use the metadata
                int samplingRate = metadata.samplingRate();
                String mappedLocation = locationMapping.hl7AdtLocationFromCapsuleLocation(locationId);
                String mappedStreamDescription = metadata.mappedStreamDescription();
                String unit = metadata.unit();

                // non-numerical types won't be able to go in the waveform table, but it's possible
                // we might need them as a VisitObservation
                String hl7Type = obx.getField(2);
                if (!Set.of("NM", "NA").contains(hl7Type)) {
                    logger.warn("Skipping stream {} with type {}, not numerical", streamId, hl7Type);
                    continue;
                }
                String allPointsStr = obx.getField(5);
                if (allPointsStr.contains("~")) {
                    throw new Hl7ParseException(partiallyParsedMessage.rawHl7Trimmed, "must only be 1 repeat in OBX-5");
                }

                List<Double> points = Arrays.stream(allPointsStr.split("\\^")).map(Double::parseDouble).toList();

                String messageIdSpecific = String.format("%s_%d_%d", messageIdBase, obrI, obxI);
                logger.debug("location {}, time {}, messageId {}, value count = {}",
                        locationId, obsDatetime, messageIdSpecific, points.size());
                WaveformMessage waveformMessage = waveformMessageFromValues(
                        samplingRate, locationId, mappedLocation, obsDatetime, messageIdSpecific,
                        streamId, mappedStreamDescription, unit, points);

                allWaveformMessages.add(waveformMessage);
            }
        }

        return new FullyParsedMessage(
                partiallyParsedMessage.rawHl7Trimmed,
                allWaveformMessages,
                pv1LocationId,
                partiallyParsedMessage.messageTimestamp,
                partiallyParsedMessage.messageTimeslot);
    }

    /**
     * Interpret timestamps as they are found in waveform HL7 messages.
     * "Basic" ISO8601 format, missing the "T", with a zone offset.
     * @param datetimeStr timestamp as string
     * @return the Instant it represents
     * @throws DateTimeException if it didn't match the expected format
     */
    private static Instant interpretWaveformTimestamp(String datetimeStr) throws DateTimeException {
        logger.trace("Parsing datetime {}", datetimeStr);
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss.SSSZZ");
        Instant obsDatetime;
        TemporalAccessor ta = formatter.parse(datetimeStr);
        obsDatetime = Instant.from(ta);
        return obsDatetime;
    }

    @SuppressWarnings("checkstyle:ParameterNumber")
    private WaveformMessage waveformMessageFromValues(
            int samplingRate, String locationId, String mappedLocation, Instant messageStartTime, String messageId,
            String sourceStreamId, String mappedStreamDescription, String unit, List<Double> arrayValues) {
        WaveformMessage waveformMessage = new WaveformMessage();
        waveformMessage.setSamplingRate(samplingRate);
        waveformMessage.setSourceLocationString(locationId);
        waveformMessage.setMappedLocationString(mappedLocation);
        waveformMessage.setMappedStreamDescription(mappedStreamDescription);
        waveformMessage.setObservationTime(messageStartTime);
        waveformMessage.setSourceMessageId(messageId);
        waveformMessage.setSourceStreamId(sourceStreamId);
        waveformMessage.setUnit(unit);
        waveformMessage.setNumericValues(new InterchangeValue<>(arrayValues));
        logger.trace("output interchange waveform message = {}", waveformMessage);
        return waveformMessage;
    }

    /**
     * Parse an HL7 message starting from text and store the resulting WaveformMessage in the queue awaiting collation.
     * If HL7 is invalid or in a form that the ad hoc parser can't handle, log error and skip.
     * Main use case for doSave = false is when you're feeding it messages that were read from your
     * saved messages in the first place.
     * @param messageAsStr One HL7 message as a string
     * @param doSave to save message or not
     * @throws Hl7ParseException if data cannot be parsed
     * @throws WaveformCollator.CollationException if the data has a logical error that prevents collation
     */
    public void saveParseQueue(String messageAsStr, boolean doSave) throws Hl7ParseException, WaveformCollator.CollationException {
        PartiallyParsedMessage partiallyParsedMessage = parseHl7Headers(messageAsStr);
        saveParseQueue(partiallyParsedMessage, doSave);
    }

    /**
     * Fully parse an HL7 message that has been partially parsed and store the resulting WaveformMessage in the queue awaiting collation.
     * If HL7 is invalid or in a form that the ad hoc parser can't handle, log error and skip.
     * Main use case for doSave = false is when you're feeding it messages that were read from your
     * saved messages in the first place.
     * @param partiallyParsedMessage One HL7 message that has been partially processed
     * @param doSave to save message or not
     *
     */
    public void saveParseQueue(PartiallyParsedMessage partiallyParsedMessage, boolean doSave) {
        if (doSave) {
            try {
                saveMessage(partiallyParsedMessage);
            } catch (IOException e) {
                // swallow the exception so that processing will still continue even if disk writing fails
                logger.error("HL7 saving failed", e);
            }
        }
        FullyParsedMessage fullyParsed = null;
        try {
            fullyParsed = parseHl7Fully(partiallyParsedMessage);
            queueForCollation(fullyParsed);
        } catch (Hl7ParseException e) {
            logger.error("HL7 parsing failed, first 100 chars: {}\nstacktrace {}",
                    e.getHl7Message().substring(0, Math.min(100, e.getHl7Message().length())),
                    e.getStackTrace());
        } catch (WaveformCollator.CollationException e) {
            logger.error("HL7 collator collation failed", e);
        }
    }

    void saveMessage(PartiallyParsedMessage partiallyParsed) throws IOException {
        /*
         * Since we're saving as individual files, save the trimmed version
         * with no separating whitespace.
         */
        String messageToSave = partiallyParsed.rawHl7Trimmed();
        Instant messageTimestamp = partiallyParsed.messageTimestamp();
        String bedId = partiallyParsed.bedLocation();
        // We can't save unless we known the time and bed ID, but also the message is so malformed that processing
        // in general is likely pointless.
        if (messageTimestamp == null || bedId == null) {
            logger.error("HL7 parsing could not find timestamp or bed ID, will not process further. First 100 chars: {}",
                    messageToSave.substring(0, Math.min(100, messageToSave.length())));
            return;
        }
        hl7MessageSaver.saveMessage(
                messageToSave,
                messageTimestamp,
                bedId);
    }

    void queueForCollation(FullyParsedMessage fullyParsed) throws WaveformCollator.CollationException {
        List<WaveformMessage> msgs = fullyParsed.waveformMessages();
        logger.trace("HL7 message generated {} Waveform messages, sending for collation", msgs.size());
        waveformCollator.addMessages(msgs);
        numHl7++;
        if (numHl7 % 5000 == 0) {
            logger.debug("Have parsed and queued {} HL7 messages in total, {} pending messages, "
                            + " {} pending samples",
                    numHl7,
                    waveformCollator.getPendingMessageCount(),
                    waveformCollator.getPendingSampleCount());
        }
    }

    @Setter
    @Getter
    private int maxCollatedMessageSamples = 3000;
    @Setter
    @Getter
    private final ChronoUnit assumedRounding = ChronoUnit.MILLIS;
    @Setter
    @Getter
    private int waitForDataLimitMillis = 15000;

    /**
     * Get collated messages, if any, and send them to the Publisher.
     * @throws InterruptedException If the Publisher thread is interrupted
     * @throws WaveformCollator.CollationException if the data has a logical error that prevents collation
     */
    @Scheduled(fixedDelay = 10 * 1000)
    public void collateAndSend() throws InterruptedException, WaveformCollator.CollationException {
        logger.debug("{} uncollated waveform messages pending", waveformCollator.pendingMessages.size());
        List<WaveformMessage> msgs = waveformCollator.getReadyMessages(
                Instant.now(), maxCollatedMessageSamples, waitForDataLimitMillis, assumedRounding);
        logger.info("{} collated waveform messages ready for sending", msgs.size());
        for (var m: msgs) {
            // consider sending to publisher in batches?
            waveformOperations.sendMessage(m);
        }
        logger.info("collateAndSend end");
    }

}
