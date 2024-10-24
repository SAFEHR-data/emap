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
import java.util.Random;
import java.util.stream.Collectors;

@Component
public class Hl7Generator {
    private final Logger logger = LoggerFactory.getLogger(Hl7Generator.class);

    @Value("${waveform.synthetic.num_patients:30}")
    private int numPatients;


    /**
     * The generator can be run in "live" or "catch-up" mode.
     *
     * Live: you are simulating the generation of data that has just happened. This will
     * be useful for the testing of waiting for data to arrive if gaps are detected.
     * startDatetime should be set to null.
     *
     * Catch-up: start at a particular point (usually in the past) and generate data at a certain rate
     * until the end date (if specified) is reached. Useful for validation runs. You would typically
     * want startDatetime and endDatetime to match the validation run start/end time.
     * In catch-up mode, warp factor can be used to control how quickly data
     * will be fed in (measure in multiples of real time).
     */
    @Value("${waveform.synthetic.start_datetime:#{null}}")
    private Instant startDatetime;
    /**
     * Where we are up to in generating data (observation time).
     */
    private Instant progressDatetime;
    /**
     * @return Where we want to be up to in generating data (observation time).
     * This value is used to generate data at the correct rate.
     * In live mode (ie. warp factor 1), this will track just behind the current time at a steady-ish rate.
     */
    private Instant getExpectedProgressDatetime() {
        long monotonicNowNanos = System.nanoTime();
        return startDatetime.plus(
                (monotonicNowNanos - monotonicStartTimeNanos) * warpFactor,
                ChronoUnit.NANOS);
    }

    // system time (not observation time) when we started running
    private Long monotonicStartTimeNanos = null;
    @Value("${waveform.synthetic.end_datetime:#{null}}")
    private Instant endDatetime;
    @Value("${waveform.synthetic.warp_factor:1}")
    private int warpFactor;

    @Value("${waveform.synthetic.tcp_client_pool_size:1}")
    private int tcpClientPoolSize;

    /**
     * defaults that need to be computed.
     */
    @PostConstruct
    public void setComputedDefaults() {
        monotonicStartTimeNanos = System.nanoTime();
        if (startDatetime == null) {
            // live mode, start generating data in the very recent past
            startDatetime = Instant.now().minus(5, ChronoUnit.SECONDS);
            progressDatetime = startDatetime;
            warpFactor = 1;
        } else {
            progressDatetime = startDatetime;
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
     * Every (approx) N seconds post a simulated batch of N seconds worth of data (times warp factor).
     * Assume a given number of patients, each with a 300Hz and a 50Hz machine.
     * @throws IOException on networking error
     */
    @Scheduled(fixedDelay = 1000)
    public void generateMessages() throws IOException {
        var start = Instant.now();
        // The warp factor is the main mechanism used to control how much data to put in now.
        // Although the scheduling interval and chunk size/count will affect what warp factor is achievable.
        // Analogy: A combination of scheduling interval, maxChunks, millisPerChunk
        // and your machine's power is the power of the heater; the warp factor is your thermostat.
        final int millisPerChunk = 5 * 1000;
        logger.info("Starting scheduled message dump (from {} for {} milliseconds)", progressDatetime, millisPerChunk);
        boolean shouldExit = false;
        final int maxChunks = 10;
        int numChunks = 0;
        Instant progressAtStart = progressDatetime;
        while (progressDatetime.isBefore(getExpectedProgressDatetime())) {
            logger.info("Making HL7 messages");
            List<String> synthMsgs = makeSyntheticWaveformMsgsAllPatients(progressDatetime, numPatients, millisPerChunk);
            logger.info("Sending {} HL7 messages", synthMsgs.size());
            // To avoid the worker threads in the reader being blocked trying to write to the
            // same location+stream at the same time, shuffling the list might be a good idea.
            // However, it can lead to fragmentation if millisPerChunk is too large,
            // because time-adjacent data might be very far apart in the list. So there might be a way to make it
            // work by doing a greater number of smaller dumps by keeping millisPerChunk below the message collation
            // threshold. Let's leave this performance tweaking until later.
//            Collections.shuffle(synthMsgs);

            /*
             * It seems like Spring Integration on the waveform-reader side won't properly multithread handlers
             * for messages that all come in on a single connection. So create multiple connections here.
             * However, in production we won't have control over the TCP client,
             * so we will have to fix the waveform-reader eventually.
             */
            try (Hl7TcpClientPool tcpClient = hl7TcpClientFactory.createTcpClientPool(tcpClientPoolSize)) {
                for (var msgStr : synthMsgs) {
                    byte[] messageBytes = msgStr.getBytes(StandardCharsets.UTF_8);
                    logger.debug("About to send message of size {} bytes", messageBytes.length);
                    logger.trace("Message = {}", messageBytes);
                    tcpClient.sendMessage(messageBytes);
                }
            }
            logger.info("Done sending {} HL7 messages", synthMsgs.size());

            progressDatetime = progressDatetime.plus(millisPerChunk, ChronoUnit.MILLIS);
            if (endDatetime != null && progressDatetime.isAfter(endDatetime)) {
                shouldExit = true;
                break;
            }

            numChunks++;
            if (numChunks > maxChunks) {
                // Let the scheduled task finish intermittently, so we don't spend forever in this loop
                // catching up (the next scheduled task will come along soon enough).
                break;
            }
        }
        var end = Instant.now();
        long procTimeMillis = start.until(end, ChronoUnit.MILLIS);
        long dataTimeMillis = progressAtStart.until(progressDatetime, ChronoUnit.MILLIS);
        logger.info("Full dump took {} milliseconds, covered {} milliseconds of data (real time factor {})",
                procTimeMillis, dataTimeMillis, ((double) dataTimeMillis) / procTimeMillis);
        if (shouldExit) {
            logger.warn("End date {} has been reached (cur={}), EXITING", endDatetime, progressDatetime);
            System.exit(0);
        }
    }

    private String applyHl7Template(long samplingRate, String locationId, Instant observationDatetime,
                                    String messageId, List<ImmutablePair<String, List<Double>>> valuesByStreamId) {
        // lines in HL7 messages must be CR terminated
        final String templateStr = """
                MSH|^~\\&|DATACAPTOR||||${messageDatetime}||ORU^R01|${messageId}|P|2.3||||||UNICODE UTF-8|\r\
                PID|\r\
                PV1||I|${locationId}|\r\
                OBR|||||||${obsDatetime}|||${locationId}|||${locationId}|\r\
                """;
        final String obxTemplate = """
                OBX|${obxI}|${dataType}|${streamId}||${valuesAsStr}||||||F||20|${obsDatetime}|\r\
                """;
        ZoneId hospitalTimezone = ZoneId.of("Europe/London");
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss.SSSZZ");
        String obsDatetime = formatter.format(observationDatetime.atZone(hospitalTimezone));
        // go for something vaguely realistic
        int milliSecondDelay = new Random().nextInt(50, 150);
        Instant messageDatetime = observationDatetime.plusMillis(milliSecondDelay);
        String messageDatetimeStr = formatter.format(messageDatetime.atZone(hospitalTimezone));
        Map<String, String> parameters = new HashMap<>();
        parameters.put("locationId", locationId);
        parameters.put("obsDatetime", obsDatetime);
        parameters.put("messageDatetime", messageDatetimeStr);
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
        String obrMsgStr = obrMsg.toString();
        if (obrMsgStr.contains("\n")) {
            throw new RuntimeException("HL7 message contains LF char; lines must be CR terminated");
        }
        return obrMsgStr;
    }

    /**
     * Make synthetic HL7 messages for a single patient and single machine, max one second per message.
     * @param locationId where the data originates from (machine/bed location)
     * @param streamId identifier for the stream
     * @param samplingRate in samples per second
     * @param numMillis number of milliseconds to produce data for
     * @param startTime observation time of the beginning of the period that the messages are to cover
     * @param maxSamplesPerMessage max samples per message (will split into multiple messages if needed)
     * @return all messages
     */
    private List<String> makeSyntheticWaveformMsgs(final String locationId,
                                                   final String streamId,
                                                   final long samplingRate,
                                                   final long numMillis,
                                                   final Instant startTime,
                                                   final long maxSamplesPerMessage
    ) {
        List<String> allMessages = new ArrayList<>();
        final long numSamples = numMillis * samplingRate / 1000;
        final double maxValue = 999;
        for (long overallSampleIdx = 0; overallSampleIdx < numSamples;) {
            long microsAfterStart = overallSampleIdx * 1000_000 / samplingRate;
            Instant messageStartTime = startTime.plus(microsAfterStart, ChronoUnit.MICROS);
            String timeStr = DateTimeFormatter.ofPattern("HHmmss").format(startTime.atOffset(ZoneOffset.UTC));
            String messageId = String.format("%s_s%s_t%s_msg%05d", locationId, streamId, timeStr, overallSampleIdx);

            var values = new ArrayList<Double>();
            for (long valueIdx = 0;
                 valueIdx < maxSamplesPerMessage && overallSampleIdx < numSamples;
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

    private List<String> possibleLocations = List.of(
            "UCHT03ICURM01", "UCHT03ICURM02", "UCHT03ICURM03", "UCHT03ICURM04", "UCHT03ICURM05", "UCHT03ICURM06",
            "UCHT03ICURM07", "UCHT03ICURM08", "UCHT03ICURM09", "UCHT03ICURM10", "UCHT03ICURM32",
            "UCHT03ICUBED11", "UCHT03ICUBED12", "UCHT03ICUBED14", "UCHT03ICUBED15", "UCHT03ICUBED16", "UCHT03ICUBED17",
            "UCHT03ICUBED18", "UCHT03ICUBED19", "UCHT03ICUBED20", "UCHT03ICUBED21", "UCHT03ICUBED23", "UCHT03ICUBED24",
            "UCHT03ICUBED25", "UCHT03ICUBED26", "UCHT03ICUBED27", "UCHT03ICUBED28", "UCHT03ICUBED29", "UCHT03ICUBED30",
            "UCHT03ICUBED31", "UCHT03ICUBED32", "UCHT03ICUBED33", "UCHT03ICUBED34", "UCHT03ICUBED35", "UCHT03ICUBED36"
            );

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
        numPatients = Math.min(numPatients, possibleLocations.size());
        for (int p = 0; p < numPatients; p++) {
            var location = possibleLocations.get(p);
            String streamId1 = "52912";
            String streamId2 = "27";
            int sizeBefore = waveformMsgs.size();
            waveformMsgs.addAll(makeSyntheticWaveformMsgs(
                    location, streamId1, 50, numMillis, startTime, 5));
            waveformMsgs.addAll(makeSyntheticWaveformMsgs(
                    location, streamId2, 300, numMillis, startTime, 10));
            int sizeAfter = waveformMsgs.size();
            logger.debug("Patient {} (location {}), generated {} messages", p, location, sizeAfter - sizeBefore);
        }

        return waveformMsgs;
    }

}
