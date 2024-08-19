package uk.ac.ucl.rits.inform.datasources.waveform_generator;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.text.StringSubstitutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Component
public class Hl7Generator {
    private final Logger logger = LoggerFactory.getLogger(Hl7Generator.class);

    @Value("${test.synthetic.num_patients:30}")
    private int numPatients;

    @Value("${test.synthetic.warp_factor:1}")
    private int warpFactor;

    /**
     * You might want startDatetime and endDatetime to match the validation run start time.
     */
    @Value("${test.synthetic.start_datetime:#{null}}")
    private Instant startDatetime;

    @Value("${test.synthetic.end_datetime:#{null}}")
    private Instant endDatetime;

    /**
     * defaults that need to be computed.
     */
    @PostConstruct
    public void setComputedDefaults() {
        if (startDatetime == null) {
            startDatetime = Instant.now();
        }
    }

    private final Hl7TcpClientFactory hl7TcpClientFactory;

    /**
     * @param hl7TcpClientFactory for sending generated messages
     */
    public Hl7Generator(Hl7TcpClientFactory hl7TcpClientFactory) {
        this.hl7TcpClientFactory = hl7TcpClientFactory;
    }


    /**
     * Every one minute post a simulated batch of one minute's worth of data (times warp factor).
     * Assume a given number of patients, each with a 300Hz and a 50Hz machine.
     * @throws IOException on networking error
     */
    @Scheduled(fixedRate = 60 * 1000)
    public void generateMessages() throws IOException {
        var start = Instant.now();
        // Usually if this method runs every N seconds, you would want to generate N
        // seconds worth of data. However, for non-live tests such as validation runs,
        // you may be processing (eg.) a week's worth of data in only a few hours,
        // so it makes sense to turn up this rate to generate about the same amount of data.
        int numMillis = 60 * 1000;
        logger.info("Starting scheduled message dump (from {} for {} milliseconds)", startDatetime, numMillis);
        boolean shouldExit = false;
        for (int warpIdx = 0; warpIdx < warpFactor; warpIdx++) {
            List<String> synthMsgs = makeSyntheticWaveformMsgsAllPatients(startDatetime, numPatients, numMillis);
            logger.info("Sending {} HL7 messages", synthMsgs.size());

            try (Hl7TcpClient tcpClient = hl7TcpClientFactory.createTcpClient()) {
                for (var msgStr : synthMsgs) {
                    byte[] messageBytes = msgStr.getBytes(StandardCharsets.UTF_8);
                    logger.info("About to send message of size {} bytes", messageBytes.length);
                    logger.trace("Message = {}", messageBytes);
                    tcpClient.sendMessage(messageBytes);
                }
            }

            startDatetime = startDatetime.plus(numMillis, ChronoUnit.MILLIS);
            if (endDatetime != null && startDatetime.isAfter(endDatetime)) {
                shouldExit = true;
                break;
            }
        }
        var end = Instant.now();
        logger.info("Full dump took {} milliseconds", start.until(end, ChronoUnit.MILLIS));
        if (shouldExit) {
            logger.info("End date {} has been reached (cur={}), EXITING", endDatetime, startDatetime);
            System.exit(0);
        }
    }

    private String applyHl7Template(long samplingRate, String locationId, Instant observationDatetime,
                                    String messageId, List<ImmutablePair<String, List<Double>>> valuesByStreamId) {
        final String templateStr = """
                MSH|^~\\&|DATACAPTOR||||20240731142108.741+0100||ORU^R01|${messageId}|P|2.3||||||UNICODE UTF-8|
                PID|
                PV1||I|${locationId}|
                OBR|||||||${obsTimeStr}|||${locationId}|||${locationId}|
                """;
        final String obxTemplate = """
                OBX|${obxI}|${dataType}|${streamId}||${valuesAsStr}||||||F||20|${obsTimeStr}|
                """;
        ZoneId hospitalTimezone = ZoneId.of("Europe/London");
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("YYYYMMddHHmmss.SSSZZ");
        // XXX: where to get message timestamp from? Just add a random bit maybe?
        // Will sampling rate have to be inferred at the other end? It's in the coding table I think...
        // message Id I think shouldn't be created here
        String obsTimeStr = formatter.format(observationDatetime.atZone(hospitalTimezone));
        Map<String, String> parameters = new HashMap<>();
        parameters.put("locationId", locationId);
        parameters.put("obsTimeStr", obsTimeStr);
        parameters.put("messageId", messageId);

        StringSubstitutor stringSubstitutor = new StringSubstitutor(parameters);
        StringBuilder obrMsg = new StringBuilder(stringSubstitutor.replace(templateStr));
        for (int obxI = 0; obxI < valuesByStreamId.size(); obxI++) {
            var streamValuePair = valuesByStreamId.get(obxI);
            List<Double> values = streamValuePair.getRight();
            String valuesAsStr = values.stream().map(d -> String.format("%.3f", d)).collect(Collectors.joining("^"));
            String dataType;
            if (values.size() == 1) {
                dataType = "NM";
            } else if (values.size() > 1) {
                dataType = "NA";
            } else {
                logger.error("Empty value array, why?");
                dataType = "";
            }
            parameters.put("obxI", Integer.toString(obxI + 1));
            parameters.put("streamId", streamValuePair.getLeft());
            parameters.put("dataType", dataType);
            parameters.put("valuesAsStr", valuesAsStr);
            obrMsg.append(stringSubstitutor.replace(obxTemplate));
        }
        return obrMsg.toString();
    }

    /**
     * Make synthetic HL7 messages for a single patient and single machine, max one second per message.
     * @param locationId where the data originates from (machine/bed location)
     * @param streamId identifier for the stream
     * @param samplingRate in samples per second
     * @param numMillis number of milliseconds to produce data for
     * @param startTime observation time of the beginning of the period that the messages are to cover
     * @param millisPerMessage max time per message (will split into multiple if needed)
     * @return all messages
     */
    private List<String> makeSyntheticWaveformMsgs(final String locationId,
                                                   final String streamId,
                                                   final long samplingRate,
                                                   final long numMillis,
                                                   final Instant startTime,
                                                   final long millisPerMessage
    ) {
        List<String> allMessages = new ArrayList<>();
        final long numSamples = numMillis * samplingRate / 1000;
        final double maxValue = 999;
        for (long overallSampleIdx = 0; overallSampleIdx < numSamples;) {
            long microsAfterStart = overallSampleIdx * 1000_000 / samplingRate;
            Instant messageStartTime = startTime.plus(microsAfterStart, ChronoUnit.MICROS);
            String timeStr = DateTimeFormatter.ofPattern("HHmmss").format(startTime.atOffset(ZoneOffset.UTC));
            String messageId = String.format("%s_t%s_msg%05d", locationId, timeStr, overallSampleIdx);

            var values = new ArrayList<Double>();
            long samplesPerMessage = samplingRate * millisPerMessage / 1000;
            for (long valueIdx = 0;
                 valueIdx < samplesPerMessage && overallSampleIdx < numSamples;
                 valueIdx++, overallSampleIdx++) {
                // a sine wave between maxValue and -maxValue
                values.add(2 * maxValue * Math.sin(overallSampleIdx * 0.01) - maxValue);
            }

            // Only one stream ID per HL7 message for the time being
            List<ImmutablePair<String, List<Double>>> valuesByStreamId = new ArrayList<>();
            valuesByStreamId.add(new ImmutablePair<>(streamId, values));
            String fullHl7message = applyHl7Template(samplingRate, locationId, messageStartTime, messageId, valuesByStreamId);
            allMessages.add(fullHl7message);
        }
        return allMessages;
    }


    /**
     * Generate synthetic waveform data for numPatients patients to cover a period of
     * numMillis milliseconds.
     * @param startTime time to start observation period
     * @param numPatients number of patients to generate for
     * @param numMillis length of observation period to generate data for
     * @return list of HL7 messages
     */
    public List<String> makeSyntheticWaveformMsgsAllPatients(
            Instant startTime, long numPatients, long numMillis) {
        List<String> waveformMsgs = new ArrayList<>();
        for (int p = 0; p < numPatients; p++) {
            var location = String.format("Bed%03d", p);
            String streamId1 = "52912";
            String streamId2 = "52913";
            final long millisPerMessage = 10000;
            int sizeBefore = waveformMsgs.size();
            waveformMsgs.addAll(makeSyntheticWaveformMsgs(
                    location, streamId1, 50, numMillis, startTime, millisPerMessage));
            waveformMsgs.addAll(makeSyntheticWaveformMsgs(
                    location, streamId2, 300, numMillis, startTime, millisPerMessage));
            int sizeAfter = waveformMsgs.size();
            logger.debug("Patient {}, generated {} messages", p, sizeAfter - sizeBefore);
        }

        return waveformMsgs;

    }



}