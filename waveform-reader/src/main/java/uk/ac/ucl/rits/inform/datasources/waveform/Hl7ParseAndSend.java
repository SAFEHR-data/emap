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

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAccessor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

@Component
public class Hl7ParseAndSend {
    private final Logger logger = LoggerFactory.getLogger(Hl7ParseAndSend.class);
    private final WaveformOperations waveformOperations;
    private final WaveformCollator waveformCollator;
    private final SourceMetadata sourceMetadata;
    private long numHl7 = 0;

    Hl7ParseAndSend(WaveformOperations waveformOperations,
                    WaveformCollator waveformCollator,
                    SourceMetadata sourceMetadata) {
        this.waveformOperations = waveformOperations;
        this.waveformCollator = waveformCollator;
        this.sourceMetadata = sourceMetadata;
    }

    List<WaveformMessage> parseHl7(String messageAsStr) throws Hl7ParseException {
        List<WaveformMessage> allWaveformMessages = new ArrayList<>();
        logger.debug("Parsing message of size {}", messageAsStr.length());
        Hl7Message message = new Hl7Message(messageAsStr);
        String messageIdBase = message.getField("MSH", 10);
        String pv1LocationId = message.getField("PV1", 3);
        String messageType = message.getField("MSH", 9);
        if (!messageType.equals("ORU^R01")) {
            throw new Hl7ParseException("Was expecting ORU^R01, got " + messageType);
        }
        List<Hl7Segment> allObr = message.getSegments("OBR");
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
                    throw new Hl7ParseException("Unexpected location " + locationId + "|" + pv1LocationId);
                }

                logger.trace("Parsing datetime {}", obsDatetimeStr);
                DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss.SSSZZ");
                TemporalAccessor ta = formatter.parse(obsDatetimeStr);
                Instant obsDatetime = Instant.from(ta);

                String streamId = obx.getField(3);
                String allPointsStr = obx.getField(5);
                if (allPointsStr.contains("~")) {
                    throw new Hl7ParseException("must only be 1 repeat in OBX-5");
                }
                List<Double> points = Arrays.stream(allPointsStr.split("\\^")).map(Double::parseDouble).toList();

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
                String mappedLocation = hl7AdtLocationFromCapsuleLocation(locationId);
                String mappedStreamDescription = metadata.mappedStreamDescription();
                String unit = metadata.unit();

                String messageIdSpecific = String.format("%s_%d_%d", messageIdBase, obrI, obxI);
                logger.debug("location {}, time {}, messageId {}, value count = {}",
                        locationId, obsDatetime, messageIdSpecific, points.size());
                WaveformMessage waveformMessage = waveformMessageFromValues(
                        samplingRate, locationId, mappedLocation, obsDatetime, messageIdSpecific,
                        streamId, mappedStreamDescription, unit, points);

                allWaveformMessages.add(waveformMessage);
            }
        }

        return allWaveformMessages;
    }

    private String hl7AdtLocationFromCapsuleLocation(String capsuleLocation) {
        // XXX: need to perform location mapping here (see Issue #41)
        return capsuleLocation;
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
        // XXX: where does the unit go?

        waveformMessage.setObservationTime(messageStartTime);
        waveformMessage.setSourceMessageId(messageId);
        waveformMessage.setSourceStreamId(sourceStreamId);
        waveformMessage.setNumericValues(new InterchangeValue<>(arrayValues));
        logger.trace("output interchange waveform message = {}", waveformMessage);
        return waveformMessage;
    }

    /**
     * Parse an HL7 message and store the resulting WaveformMessage in the queue awaiting collation.
     * @param messageAsStr One HL7 message as a string
     * @throws Hl7ParseException if HL7 is invalid or in a form that the ad hoc parser can't handle
     * @throws WaveformCollator.CollationException if the data has a logical error that prevents collation
     */
    public void parseAndQueue(String messageAsStr) throws Hl7ParseException, WaveformCollator.CollationException {
        List<WaveformMessage> msgs = parseHl7(messageAsStr);

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
