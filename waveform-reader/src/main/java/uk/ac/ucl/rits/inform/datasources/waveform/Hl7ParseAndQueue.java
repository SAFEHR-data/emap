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
import uk.ac.ucl.rits.inform.interchange.visit_observations.WaveformBaseMessage;
import uk.ac.ucl.rits.inform.interchange.visit_observations.WaveformHighFreqMessage;
import uk.ac.ucl.rits.inform.interchange.visit_observations.WaveformLowFreqMessage;

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
 * Receive HL7 messages, transform each to an interchange message, then either
 * queue high-frequency waveforms for collation (see {@link WaveformCollator})
 * or send low-frequency messages immediately via {@link WaveformOperations}.
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
                     Hl7MessageTimeSlotCalculator hl7MessageTimeSlotCalculator,
                     Hl7MessageSaver hl7MessageSaver) {
        this.waveformOperations = waveformOperations;
        this.waveformCollator = waveformCollator;
        this.sourceMetadata = sourceMetadata;
        this.hl7MessageTimeSlotCalculator = hl7MessageTimeSlotCalculator;
        this.hl7MessageSaver = hl7MessageSaver;
        this.locationMapping = new LocationMapping();
    }

    public record PartiallyParsedMessage(
            String rawHl7Trimmed,
            Hl7Message hl7MessageParser, // to allow for continued parsing
            String bedLocation,
            Instant messageTimestamp,
            Instant messageTimeslot) {}

    public record FullyParsedMessage(
            String rawHl7Trimmed,
            List<WaveformBaseMessage> waveformBaseMessages,
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
        List<WaveformBaseMessage> allWaveformMessages = new ArrayList<>();
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

                // aka stream ID
                String variableId = obx.getField(3);

                Optional<SourceMetadataItem> metadataOpt = sourceMetadata.getVariableMetadata(variableId);
                if (metadataOpt.isEmpty()) {
                    logger.warn("Skipping variable {}, unrecognised variableID", variableId);
                    continue;
                }
                SourceMetadataItem metadata = metadataOpt.get();
                if (!metadata.isUsable()) {
                    logger.warn("Skipping variable {}, insufficient metadata", variableId);
                    continue;
                }

                // If it's a no-channel flavour of HL7 message, OBR-13 is the location and we
                // shouldn't treat it as the channel!
                String channelId = metadata.hasChannels() ? obr.getField(13) : null;
                String mappedLocation = locationMapping.hl7AdtLocationFromCapsuleLocation(locationId);
                String messageIdSpecific = String.format("%s_%d_%d", messageIdBase, obrI, obxI);
                // Sampling rate and variable description are not in the message, so use the metadata
                String mappedVariableDescription = metadata.mappedVariableDescription();
                // Units can vary even within the same variable, so use the values in the HL7 messages in preference to the
                // ones in metadata.
                String unitCode = obx.getField(6);
                String unit = sourceMetadata.getUnitFromCode(unitCode).orElse(metadata.unit());
                String hl7Type = obx.getField(2);

                if (metadata.isWaveform()) {
                    int samplingRate = metadata.samplingRate();

                    if (!Set.of("NM", "NA").contains(hl7Type)) {
                        logger.warn("Skipping variable {} with type {}, not numerical", variableId, hl7Type);
                        continue;
                    }
                    String allPointsStr = obx.getField(5);
                    if (allPointsStr.contains("~")) {
                        throw new Hl7ParseException(partiallyParsedMessage.rawHl7Trimmed, "must only be 1 repeat in OBX-5");
                    }

                    List<Double> points = Arrays.stream(allPointsStr.split("\\^")).map(Double::parseDouble).toList();
                    logger.debug("location {}, time {}, messageId {}, value count = {}",
                            locationId, obsDatetime, messageIdSpecific, points.size());

                    WaveformHighFreqMessage waveformMessage = new WaveformHighFreqMessage();
                    setBaseFields(
                            waveformMessage, locationId, mappedLocation, obsDatetime, messageIdSpecific, variableId, mappedVariableDescription, unit);
                    setWaveformFields(waveformMessage, samplingRate, channelId, points);
                    allWaveformMessages.add(waveformMessage);
                } else {
                    WaveformLowFreqMessage lfMessage = new WaveformLowFreqMessage();
                    setBaseFields(lfMessage, locationId, mappedLocation, obsDatetime, messageIdSpecific, variableId, mappedVariableDescription, unit);
                    String sourceValue = obx.getField(5);
                    // depending on the variableId, sourceValue might be a category code or a numeric field
                    lfMessage.setSourceValue(new InterchangeValue<>(sourceValue));
                    Optional<String> mappedCategory;
                    try {
                        mappedCategory = sourceMetadata.tryMapCategorical(variableId, sourceValue);
                    } catch (UnknownCategoricalValueException e) {
                        logger.error("Skipping OBX line, failed mapping variable {} with value {}", variableId, sourceValue, e);
                        continue;
                    }
                    if (mappedCategory.isPresent()) {
                        lfMessage.setStringValue(new InterchangeValue<>(mappedCategory.get()));
                    } else {
                        // not a known categorical, treat it as a numeric/string
                        if (hl7Type.equals("ST")) {
                            lfMessage.setStringValue(new InterchangeValue<>(sourceValue));
                        } else if (hl7Type.equals("NM")) {
                            try {
                                Double numericValue = Double.parseDouble(sourceValue);
                                lfMessage.setNumericValue(new InterchangeValue<>(numericValue));
                            } catch (NumberFormatException e) {
                                logger.error("Skipping OBX line, invalid number {}", sourceValue, e);
                                continue;
                            }
                        } else {
                            logger.error("Skipping OBX line, cannot handle HL7 data type {} for variable {}", hl7Type, variableId);
                            continue;
                        }
                    }
                    allWaveformMessages.add(lfMessage);
                }
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
    private void setBaseFields(WaveformBaseMessage message, String locationId, String mappedLocation, Instant messageStartTime, String messageId,
                               String sourceVariableId, String mappedVariableDescription, String unit) {
        message.setSourceMessageId(messageId);
        message.setSourceLocationString(locationId);
        message.setMappedLocationString(mappedLocation);
        message.setMappedVariableDescription(mappedVariableDescription);
        message.setObservationTime(messageStartTime);
        message.setSourceVariableId(sourceVariableId);
        message.setUnit(unit);

    }

    @SuppressWarnings("checkstyle:ParameterNumber")
    private void setWaveformFields(
            WaveformHighFreqMessage waveformMessage,
            int samplingRate, String sourceChannelId, List<Double> arrayValues) {
        waveformMessage.setSamplingRate(samplingRate);
        waveformMessage.setSourceChannelId(sourceChannelId);
        waveformMessage.setNumericValues(new InterchangeValue<>(arrayValues));
        logger.trace("output interchange WaveformMessage = {}", waveformMessage);
    }


    /**
     * Parse an HL7 message starting from text, optionally save it, then dispatch the resulting
     * interchange messages (queue HF for collation, send LF immediately).
     * If HL7 is invalid or in a form that the ad hoc parser can't handle, log error and skip.
     * Main use case for doSave = false is when you're feeding it messages that were read from your
     * saved messages in the first place.
     * @param messageAsStr One HL7 message as a string
     * @param doSave to save message or not
     * @throws Hl7ParseException if header parsing fails before dispatch
     */
    public void saveParseQueue(String messageAsStr, boolean doSave) throws Hl7ParseException {
        PartiallyParsedMessage partiallyParsedMessage = parseHl7Headers(messageAsStr);
        saveParseQueue(partiallyParsedMessage, doSave);
    }

    /**
     * Fully parse an HL7 message that has been partially parsed, then dispatch the resulting
     * interchange messages (queue HF for collation, send LF immediately).
     * If HL7 is invalid or in a form that the ad hoc parser can't handle, log error and skip.
     * Main use case for doSave = false is when you're feeding it messages that were read from your
     * saved messages in the first place.
     * @param partiallyParsedMessage One HL7 message that has been partially processed
     * @param doSave to save message or not
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
        try {
            FullyParsedMessage fullyParsed = parseHl7Fully(partiallyParsedMessage);
            dispatchMessages(fullyParsed);
        } catch (Hl7ParseException e) {
            logger.error("HL7 parsing failed, first 100 chars: {}\nstacktrace {}",
                    e.getHl7Message().substring(0, Math.min(100, e.getHl7Message().length())),
                    e.getStackTrace());
        } catch (WaveformCollator.CollationException e) {
            logger.error("HL7 collator collation failed", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.warn("Publish interrupted; abandoning further dispatch for this HL7 message", e);
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

    /**
     * Queue high-frequency waveform messages for collation, and send low-frequency messages immediately.
     * @param fullyParsed fully parsed HL7 message
     * @throws WaveformCollator.CollationException if the data has a logical error that prevents collation
     * @throws InterruptedException if publishing is interrupted while waiting to enqueue
     */
    void dispatchMessages(FullyParsedMessage fullyParsed)
            throws WaveformCollator.CollationException, InterruptedException {
        // it would be very unexpected for an HL7 message to have a mixture of HF and LF data.
        List<WaveformBaseMessage> msgs = fullyParsed.waveformBaseMessages();
        List<WaveformHighFreqMessage> hfMessages = msgs.stream()
                .filter(msg -> msg instanceof WaveformHighFreqMessage)
                .map(msg -> (WaveformHighFreqMessage) msg)
                .toList();
        List<WaveformLowFreqMessage> lfMessages = msgs.stream()
                .filter(msg -> (msg instanceof WaveformLowFreqMessage))
                .map(msg -> (WaveformLowFreqMessage) msg)
                .toList();
        logger.trace("HL7 message generated {} Waveform messages ({} collatable, {} not), sending for collation",
                msgs.size(), hfMessages.size(), lfMessages.size());
        for (var m: lfMessages) {
            waveformOperations.sendMessage(m);
        }
        waveformCollator.addMessages(hfMessages);
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
     * All Scheduling is disabled for HL7 replay, so you need to call this manually.
     * @throws InterruptedException If the Publisher thread is interrupted
     * @throws WaveformCollator.CollationException if the data has a logical error that prevents collation
     */
    @Scheduled(fixedDelay = 10 * 1000)
    public void collateAndSend() throws InterruptedException, WaveformCollator.CollationException {
        logger.debug("{} uncollated waveform messages pending", waveformCollator.pendingMessages.size());
        List<WaveformHighFreqMessage> msgs = waveformCollator.getReadyMessages(
                Instant.now(), maxCollatedMessageSamples, waitForDataLimitMillis, assumedRounding);
        logger.info("{} collated waveform messages ready for sending", msgs.size());
        for (var m: msgs) {
            // consider sending to publisher in batches?
            waveformOperations.sendMessage(m);
        }
        logger.info("collateAndSend end");
    }

}
