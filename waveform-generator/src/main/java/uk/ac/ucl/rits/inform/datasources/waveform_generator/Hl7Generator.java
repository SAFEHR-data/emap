package uk.ac.ucl.rits.inform.datasources.waveform_generator;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.text.StringSubstitutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import uk.ac.ucl.rits.inform.datasources.waveform.LocationMapping;
import uk.ac.ucl.rits.inform.datasources.waveform_generator.patient_model.PatientLocationModel;
import uk.ac.ucl.rits.inform.interchange.EmapOperationMessage;
import uk.ac.ucl.rits.inform.interchange.adt.AdtMessage;
import uk.ac.ucl.rits.inform.interchange.messaging.Publisher;

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

    private final Publisher publisher;

    @Value("${waveform.synthetic.num_patients:30}")
    private int numPatients;

    private final GeneratorContext generatorContext = new GeneratorContext();

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

    private final LocationMapping locationMapping = new LocationMapping();

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
        // need to initialise with the fact time so ADT correlates with waveform times
        patientLocationModel = new PatientLocationModel(possibleLocations, progressDatetime);
    }

    private final Hl7TcpClientFactory hl7TcpClientFactory;

    /**
     * @param hl7TcpClientFactory for sending generated messages
     * @param publisher for sending synthetic ADT messages
     */
    public Hl7Generator(Hl7TcpClientFactory hl7TcpClientFactory, Publisher publisher) {
        this.hl7TcpClientFactory = hl7TcpClientFactory;
        this.publisher = publisher;
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
            logger.info("Making HL7 messages starting at time {} for {} milliseconds", progressDatetime, millisPerChunk);
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
     * @param signalFrequencyHz the signal baseline frequency (Hz)
     * @param numMillis number of milliseconds to produce data for
     * @param startTime observation time of the beginning of the period that the messages are to cover
     * @param maxSamplesPerMessage max samples per message (will split into multiple messages if needed)
     * @return all messages
     */
    private List<String> makeSyntheticWaveformMsgs(final String locationId,
                                                   final String streamId,
                                                   final long samplingRate,
                                                   final double signalFrequencyHz,
                                                   final long numMillis,
                                                   final Instant startTime,
                                                   final long maxSamplesPerMessage
    ) {
        List<String> allMessages = new ArrayList<>();
        final long numSamplesThisCall = numMillis * samplingRate / 1000;
        final double maxValue = 999;
        GeneratorContext.GeneratorContextRecord context = generatorContext.getContext(locationId, streamId);
        // This counter persists over repeated calls to this function to avoid the input
        // to sin being reset to zero every few seconds
        long persistentSampleIdx = context.getCounter();
        for (int thisCallCounter = 0; thisCallCounter < numSamplesThisCall;) {
            long microsAfterStart = thisCallCounter * 1000_000L / samplingRate;
            Instant messageStartTime = startTime.plus(microsAfterStart, ChronoUnit.MICROS);
            String timeStr = DateTimeFormatter.ofPattern("HHmmss").format(startTime.atOffset(ZoneOffset.UTC));
            String messageId = String.format("%s_s%s_t%s_msg%05d", locationId, streamId, timeStr, persistentSampleIdx);

            var values = new ArrayList<Double>();
            for (long valueIdx = 0;
                 valueIdx < maxSamplesPerMessage && thisCallCounter < numSamplesThisCall;
                 valueIdx++, thisCallCounter++, persistentSampleIdx++) {
                values.add(maxValue * Math.sin(2 * Math.PI * signalFrequencyHz * persistentSampleIdx / samplingRate));
            }

            // Only one stream ID per HL7 message for the time being
            List<ImmutablePair<String, List<Double>>> valuesByStreamId = new ArrayList<>();
            valuesByStreamId.add(new ImmutablePair<>(streamId, values));
            String fullHl7message = applyHl7Template(samplingRate, locationId, messageStartTime, messageId, valuesByStreamId);
            allMessages.add(fullHl7message);
        }
        context.setCounter(persistentSampleIdx);
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

    private PatientLocationModel patientLocationModel = null;

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
        List<SyntheticStream> syntheticStreams = List.of(
                new SyntheticStream("52912", 50, 0.3, 5), // airway volume
                new SyntheticStream("27", 300, 1.2, 10) // ECG
        );
        List<AdtMessage> locationChangeMessages = patientLocationModel.makeModifications(startTime);
        submitBatch(locationChangeMessages);

        List<String> empties = new ArrayList<>();
        for (int p = 0; p < numPatients; p++) {
            String location = possibleLocations.get(p);
            if (patientLocationModel.getPatientForLocation(location) == null) {
                empties.add(location);
                continue;
            }
            int sizeBefore = waveformMsgs.size();
            // each bed has a slightly different frequency
            double frequencyFactor =  0.95 + 0.1 * p / possibleLocations.size();
            // don't turn on all streams for all patients to test more realistically
            long streamsEnabledBitPattern = ~p; // p = 0 has all streams enabled, etc
            for (int si = 0; si < syntheticStreams.size(); si++) {
                boolean thisStreamEnabled = 0 != ((streamsEnabledBitPattern >> si) & 1);
                if (!thisStreamEnabled) {
                    continue;
                }
                SyntheticStream stream = syntheticStreams.get(si);
                waveformMsgs.addAll(makeSyntheticWaveformMsgs(
                        location, stream.streamId, stream.samplingRate,
                        stream.baselineSignalFrequency * frequencyFactor, numMillis, startTime, stream.maxSamplesPerMessage));
            }
            int sizeAfter = waveformMsgs.size();
            logger.debug("Patient {} (location {}), generated {} messages (incl ADT)", p, location, sizeAfter - sizeBefore);
        }
        logger.info("Not generating data for empty locations: {}", empties);

        return waveformMsgs;
    }

    private void submitBatch(List<AdtMessage> adtMsgs) {
        List<ImmutablePair<EmapOperationMessage, String>> batch = new ArrayList<>();
        int i = 0;
        for (var adt: adtMsgs) {
            i++;
            batch.add(new ImmutablePair<>(adt, String.format("%s_message_%d",
                    Instant.now().toEpochMilli(), i)));
        }
        if (batch.isEmpty()) {
            return;
        }
        try {
            String batchId = batch.get(0).getRight();
            publisher.submit(batch, batchId, () -> {
                logger.info("Successfully submitted batch {} (size {})", batchId, batch.size());
            });
        } catch (InterruptedException e) {
            logger.error("submit interrupted", e);
        }
    }

    record SyntheticStream(String streamId, int samplingRate, double baselineSignalFrequency, int maxSamplesPerMessage) {
    }

    private class GeneratorContext {
        private final Map<ImmutablePair<String, String>, GeneratorContextRecord> allContexts = new HashMap<>();

        @AllArgsConstructor
        class GeneratorContextRecord {
            @Getter @Setter
            private long counter;
        }

        public GeneratorContextRecord getContext(ImmutablePair<String, String> contextKey) {
            return allContexts.computeIfAbsent(contextKey, k -> new GeneratorContextRecord(0));
        }

        public GeneratorContextRecord getContext(String locationId, String streamId) {
            return getContext(GeneratorContext.makeKey(locationId, streamId));
        }

        public static ImmutablePair<String, String> makeKey(String locationId, String streamId) {
            return new ImmutablePair<>(locationId, streamId);
        }
    }

}
