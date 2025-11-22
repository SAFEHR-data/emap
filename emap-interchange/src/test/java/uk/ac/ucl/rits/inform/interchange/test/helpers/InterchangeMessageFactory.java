package uk.ac.ucl.rits.inform.interchange.test.helpers;


import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectReader;
import lombok.NoArgsConstructor;
import org.springframework.lang.Nullable;
import uk.ac.ucl.rits.inform.interchange.AdvanceDecisionMessage;
import uk.ac.ucl.rits.inform.interchange.ConsultMetadata;
import uk.ac.ucl.rits.inform.interchange.ConsultRequest;
import uk.ac.ucl.rits.inform.interchange.FileStoreWithMonitoredAccess;
import uk.ac.ucl.rits.inform.interchange.InterchangeValue;
import uk.ac.ucl.rits.inform.interchange.PatientAllergy;
import uk.ac.ucl.rits.inform.interchange.PatientInfection;
import uk.ac.ucl.rits.inform.interchange.PatientProblem;
import uk.ac.ucl.rits.inform.interchange.ResearchOptOut;
import uk.ac.ucl.rits.inform.interchange.adt.AdtMessage;
import uk.ac.ucl.rits.inform.interchange.form.FormMetadataMsg;
import uk.ac.ucl.rits.inform.interchange.form.FormMsg;
import uk.ac.ucl.rits.inform.interchange.form.FormQuestionMetadataMsg;
import uk.ac.ucl.rits.inform.interchange.lab.LabIsolateMsg;
import uk.ac.ucl.rits.inform.interchange.lab.LabMetadataMsg;
import uk.ac.ucl.rits.inform.interchange.lab.LabOrderMsg;
import uk.ac.ucl.rits.inform.interchange.lab.LabResultMsg;
import uk.ac.ucl.rits.inform.interchange.location.DepartmentMetadata;
import uk.ac.ucl.rits.inform.interchange.location.LocationMetadata;
import uk.ac.ucl.rits.inform.interchange.visit_observations.Flowsheet;
import uk.ac.ucl.rits.inform.interchange.visit_observations.FlowsheetMetadata;
import uk.ac.ucl.rits.inform.interchange.visit_observations.WaveformMessage;
import uk.ac.ucl.rits.inform.interchange.NotesMetadataMessage;

import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;

import static uk.ac.ucl.rits.inform.interchange.utils.DateTimeUtils.roundInstantToNearest;


/**
 * Builds interchange messages from yaml files.
 * Allows for easier setup for integration testing in hl7 sources and EMAP star
 */
@NoArgsConstructor
public class InterchangeMessageFactory {
    public FileStoreWithMonitoredAccess fileStore = null;

    private static final String sourceId = "0000000042";

    /**
     * Create a message factory with monitored files, enabling the resources accessible to this class to be
     * queried for if they have, or not, been accessed
     * @return Interchange message factory
     * @throws URISyntaxException If the file store cannot be created
     * @throws IOException If the file store cannot be created
     */
    public static InterchangeMessageFactory withMonitoredFiles() throws URISyntaxException, IOException {
        var factory = new InterchangeMessageFactory();
        factory.fileStore = new FileStoreWithMonitoredAccess();

        return factory;
    }

    /**
     * Builds an notesMetadata message from yaml file.
     * @param fileName filename within test resources/NotesMetadataMessages
     * @return NotesMetadata message
     * @throws IOException if files don't exist
     */
    public <T extends NotesMetadataMessage> T getNotesMetadataMessage(final String fileName) throws IOException {
        String resourcePath = "/NotesMetadata/" + fileName;
        InputStream inputStream = getInputStream(resourcePath);

        return EmapYamlMapper.readValue(inputStream, new TypeReference<>() {});
    }

    /**
     * Builds an ADT message from yaml file.
     * @param fileName filename within test resources/AdtMessages
     * @return ADT message
     * @throws IOException if files don't exist
     */
    public <T extends AdtMessage> T getAdtMessage(final String fileName) throws IOException {
        String resourcePath = "/AdtMessages/" + fileName;
        InputStream inputStream = getInputStream(resourcePath);

        return EmapYamlMapper.readValue(inputStream, new TypeReference<>() {});
    }

    /**
     * @param fileName filename within the ConsultRequest folder
     * @return consult message
     * @throws IOException if the file doesn't exist
     */
    public ConsultRequest getConsult(final String fileName) throws IOException {
        InputStream inputStream = getInputStream(String.format("/ConsultRequest/%s", fileName));
        return EmapYamlMapper.readValue(inputStream, new TypeReference<>() {});
    }

    /**
     * @param fileName filename within the ConsultMetadata folder
     * @return consult message
     * @throws IOException if the file doesn't exist
     */
    public ConsultMetadata getConsultMetadata(final String fileName) throws IOException {
        InputStream inputStream = getInputStream(String.format("/ConsultMetadata/%s", fileName));
        return EmapYamlMapper.readValue(inputStream, new TypeReference<>() {});
    }

    /**
     * @param fileName filename within the AdvancedDecisions messages folder
     * @return advanced decision message
     * @throws IOException if the file doesn't exist
     */
    public AdvanceDecisionMessage getAdvanceDecision(final String fileName) throws IOException {
        InputStream inputStream = getInputStream(String.format("/AdvanceDecision/%s", fileName));
        return EmapYamlMapper.readValue(inputStream, new TypeReference<>() {});
    }

    /**
     * Builds notes metadata from the file given.
     * 
     * @param fileName filename within the notes metadata messages folder
     * @return note metadata message
     * @throws IOException if the file doesn't exist
     */
    public NotesMetadataMessage getNoteMessage(final String fileName) throws IOException {
        InputStream inputStream = getInputStream(String.format("/Notes/%s", fileName));
        return EmapYamlMapper.readValue(inputStream, new TypeReference<>() {});
    }

    /**
     * Builds lab orders from yaml file given, overriding default values for lab orders and lab results
     * @param fileName            filename in test resources/LabOrders,
     *                            defaults are '{file_stem}_order_defaults.yaml' and '{file_stem}_order_defaults.yaml'
     *                            sensitivity defaults are '{file_stem}_sens_order_defaults.yaml' and '{file_stem}_sens_order_defaults.yaml'
     * @param sourceMessagePrefix message prefix
     * @return List of lab orders deserialised from the files
     * @throws IOException if files don't exist
     */
    public List<LabOrderMsg> getLabOrders(final String fileName, final String sourceMessagePrefix) throws IOException {
        String resourcePath = "/LabOrders/" + fileName;
        InputStream inputStream = getInputStream(resourcePath);
        List<LabOrderMsg> labOrderMsgs = EmapYamlMapper.readValue(inputStream, new TypeReference<>() {});
        int count = 1;
        for (LabOrderMsg order : labOrderMsgs) {
            String sourceMessageId = sourceMessageIdWithCount(sourceMessagePrefix, count);
            updateLabOrderAndResults(order, sourceMessageId, resourcePath.replace(".yaml", ""));
            updateLabIsolates(order, resourcePath.replace(".yaml", "_micro"));
            count++;
        }

        return labOrderMsgs;
    }

    private static String sourceMessageIdWithCount(String sourceMessagePrefix, int count) {
        return String.format("%s_%02d", sourceMessagePrefix, count);
    }


    /**
     * Overloaded getLabOrders method with the a default message prefix
     */
    public List<LabOrderMsg> getLabOrders(final String fileName) throws IOException {
        return getLabOrders(fileName, sourceId);
    }

    /**
     * Get lab order from single oru_r01 yaml file.
     * @param filePath filepath from lab orders
     * @return Lab order message
     * @throws IOException if files don't exist
     */
    public LabOrderMsg getLabOrder(final String filePath) throws IOException {
        String resourcePath = String.format("/LabOrders/%s", filePath);
        InputStream inputStream = getInputStream(resourcePath);
        return EmapYamlMapper.readValue(inputStream, new TypeReference<>() {});
    }

    public List<PatientInfection> getPatientInfections(final String fileName) throws IOException {
        String resourcePath = "/PatientInfection/" + fileName;
        InputStream inputStream = getInputStream(resourcePath);
        return EmapYamlMapper.readValue(inputStream, new TypeReference<>() {});
    }

    public List<PatientProblem> getPatientProblems(final String fileName) throws IOException {
        String resourcePath = "/PatientProblem/" + fileName;
        InputStream inputStream = getInputStream(resourcePath);
        return EmapYamlMapper.readValue(inputStream, new TypeReference<>() {});
    }

    public List<PatientAllergy> getPatientAllergies(final String fileName) throws IOException {
        String resourcePath = "/PatientAllergies/" + fileName;
        InputStream inputStream = getInputStream(resourcePath);
        return EmapYamlMapper.readValue(inputStream, new TypeReference<>() {});
    }

    /**
     * Build flowsheet metadata expected data from specified file.
     * @param fileName the file from which to build the data
     * @return the data as expected Interchange messages
     * @throws IOException if reading failed
     */
    public List<FlowsheetMetadata> getFlowsheetMetadata(final String fileName) throws IOException {
        String resourcePath = "/FlowsheetMetadata/" + fileName;
        InputStream inputStream = getInputStream(resourcePath);
        return EmapYamlMapper.readValue(inputStream, new TypeReference<>() {});
    }

    /**
     * Build location metadata expected data from specified file.
     * @param fileName the file from which to build the data
     * @return the data as expected Interchange message
     * @throws IOException if reading failed
     */
    public LocationMetadata getLocationMetadata(final String fileName) throws IOException {
        String resourcePath = "/LocationMetadata/" + fileName;
        InputStream inputStream = getInputStream(resourcePath);
        return EmapYamlMapper.readValue(inputStream, new TypeReference<>() {});
    }

    /**
     * Build Department metadata expected data from specified file.
     * @param fileName the file from which to build the data
     * @return the data as expected Interchange message
     * @throws IOException if reading failed
     */
    public DepartmentMetadata getDepartmentMetadata(final String fileName) throws IOException {
        String resourcePath = "/DepartmentMetadata/" + fileName;
        InputStream inputStream = getInputStream(resourcePath);
        return EmapYamlMapper.readValue(inputStream, new TypeReference<>() {});
    }

    public List<LabMetadataMsg> getLabMetadataMsgs(final String fileName) throws IOException {
        InputStream resourceAsStream = getInputStream("/LabsMetadata/" + fileName);
        return EmapYamlMapper.readValue(resourceAsStream, new TypeReference<>() {});
    }

    /**
     * Build interchange message from file.
     * @param fileName filename of the yaml serialised data
     * @return interchange message of the data
     * @throws IOException if the file can't be read
     */
    public List<ResearchOptOut> getResearchOptOuts(String fileName) throws IOException {
        InputStream resourceAsStream = getInputStream("/ResearchOptOut/" + fileName);
        return EmapYamlMapper.readValue(resourceAsStream, new TypeReference<>() {});
    }

    /**
     * Builds Flowsheets from yaml file given, overriding default values from '{file_stem}_defaults.yaml'
     * @param fileName            yaml filename in test resources/Flowsheets, default values from '{file_stem}_defaults.yaml'
     * @param sourceMessagePrefix message prefix
     * @return List of Flowsheets
     * @throws IOException if files don't exist
     */
    public List<Flowsheet> getFlowsheets(final String fileName, final String sourceMessagePrefix) throws IOException {
        String resourcePath = "/Flowsheets/" + fileName;
        InputStream inputStream = getInputStream(resourcePath);
        List<Flowsheet> flowsheets = EmapYamlMapper.readValue(inputStream, new TypeReference<>() {});
        int count = 1;
        for (Flowsheet flowsheet : flowsheets) {
            String sourceMessageId = sourceMessageIdWithCount(sourceMessagePrefix, count);
            flowsheet.setSourceMessageId(sourceMessageId);

            // update order with yaml data
            ObjectReader orderReader = EmapYamlMapper.readerForUpdating(flowsheet);
            String orderDefaultPath = resourcePath.replace(".yaml", "_defaults.yaml");
            orderReader.readValue(getInputStream(orderDefaultPath));

            count++;
        }
        return flowsheets;
    }

    /**
     * Overloaded getFlowsheets method with the a default message prefix
     */
    public List<Flowsheet> getFlowsheets(final String fileName) throws IOException {
        return getFlowsheets(fileName, sourceId);
    }

    /**
     *
     * @param sourceStreamId how the source data identifies this stream
     * @param mappedStreamName how the reader has interpreted the source stream id
     * @param samplingRate samples per second
     * @param numSamples total bumber of samples to generate
     * @param maxSamplesPerMessage how many samples to put in a message; split as necessary
     * @param sourceLocation bed location according to the original data
     * @param mappedLocation bed location according to data reader's interpretation of the original data
     * @param obsDatetime when the data occurred
     * @param unit the unit of the measurement
     * @param roundToUnit what precision to round obsDatetime to when creating messages (to be more realistic),
     *                    or null to not perform rounding
     * @return list of messages containing synthetic data
     */
    public List<WaveformMessage> getWaveformMsgs(String sourceStreamId, String mappedStreamName,
                                                 int samplingRate, final int numSamples, int maxSamplesPerMessage,
                                                 String sourceLocation, String mappedLocation,
                                                 Instant obsDatetime, String unit, ChronoUnit roundToUnit) {
        // XXX: perhaps make use of the hl7-reader utility function for splitting messages? Or is that cheating?
        // Or should such a utility function go into (non-test) Interchange?
        List<WaveformMessage> allMessages = new ArrayList<>();
        int samplesRemaining = numSamples;
        while (samplesRemaining > 0) {
            int samplesThisMessage = Math.min(samplesRemaining, maxSamplesPerMessage);
            WaveformMessage waveformMessage = new WaveformMessage();
            waveformMessage.setSourceStreamId(sourceStreamId);
            waveformMessage.setMappedStreamDescription(mappedStreamName);
            waveformMessage.setSamplingRate(samplingRate);
            waveformMessage.setSourceLocationString(sourceLocation);
            waveformMessage.setMappedLocationString(mappedLocation);
            var values = new ArrayList<Double>();
            for (int i = 0; i < samplesThisMessage; i++) {
                values.add(Math.sin((numSamples - samplesRemaining + i) * 0.01));
            }
            waveformMessage.setUnit(unit);
            waveformMessage.setNumericValues(new InterchangeValue<>(values));
            Instant obsDatetimeRounded = roundInstantToNearest(obsDatetime, roundToUnit);
            waveformMessage.setObservationTime(obsDatetimeRounded);
            allMessages.add(waveformMessage);
            samplesRemaining -= samplesThisMessage;
            obsDatetime = obsDatetime.plus(samplesThisMessage * 1000_000L / samplingRate, ChronoUnit.MICROS);
        }
        return allMessages;
    }

    /**
     * Utility wrapper for calling updateLabResults without updating the resultTime or epicCareOrderNumber.
     * @param results            lab results to update
     * @param resourcePathPrefix prefix in the form '{directory}/{file_stem}'
     * @throws IOException if files don't exist
     */
    private void updateLabResults(Iterable<LabResultMsg> results, final String resourcePathPrefix) throws IOException {
        updateLabResults(results, resourcePathPrefix, null, null);
    }

    /**
     * Update all of lab results from yaml file
     * @param results             lab results to update
     * @param resourcePathPrefix  prefix in the form '{directory}/{file_stem}'
     * @param resultTime          optional result time to add
     * @param epicCareOrderNumber optional epic care order number to add
     * @throws IOException if files don't exist
     */
    private void updateLabResults(Iterable<LabResultMsg> results, final String resourcePathPrefix,
                                  @Nullable Instant resultTime, @Nullable String epicCareOrderNumber)
            throws IOException {
        String resultDefaultPath = resourcePathPrefix + "_result_defaults.yaml";
        for (LabResultMsg result : results) {
            // update the epic order number and result time if either are set
            if (epicCareOrderNumber != null || resultTime != null) {
                result.setEpicCareOrderNumber(epicCareOrderNumber);
                result.setResultTime(resultTime);
            }

            // update result with yaml data
            ObjectReader resultReader = EmapYamlMapper.readerForUpdating(result);
            resultReader.readValue(getInputStream(resultDefaultPath));
        }
    }

    /**
     * Update a lab order and its lab results from yaml defaults files
     * @param order              lab order
     * @param sourceMessageId    messageId
     * @param resourcePathPrefix prefix in the form '{directory}/{file_stem}'
     * @throws IOException if files don't exist
     */
    private void updateLabOrderAndResults(LabOrderMsg order, final String sourceMessageId, final String resourcePathPrefix) throws IOException {
        order.setSourceMessageId(sourceMessageId);
        // update order with yaml data
        ObjectReader orderReader = EmapYamlMapper.readerForUpdating(order);
        String orderDefaultPath = resourcePathPrefix + "_order_defaults.yaml";
        order = orderReader.readValue(getInputStream(orderDefaultPath));
        String epicOrderNumber = order.getEpicCareOrderNumber().isSave() ? order.getEpicCareOrderNumber().get() : null;
        updateLabResults(order.getLabResultMsgs(), resourcePathPrefix, order.getStatusChangeTime(), epicOrderNumber);

    }

    /**
     * If a lab order's results has isolates, update the sensitivity with default values
     * @param order        lab order
     * @param resourcePath resource path in form '{directory}/{file_stem}_micro_'
     * @throws IOException if file doesn't exist
     */
    private void updateLabIsolates(LabOrderMsg order, final String resourcePath) throws IOException {
        for (LabResultMsg result : order.getLabResultMsgs()) {
            updateLabIsolateAndSensitivities(result.getLabIsolate(), resourcePath);
        }
    }

    /**
     * Update a lab order and its lab results from yaml defaults files
     * @param isolateMsg         lab isolate message
     * @param resourcePathPrefix prefix in the form '{directory}/{file_stem}'
     * @throws IOException if files don't exist
     */
    private void updateLabIsolateAndSensitivities(LabIsolateMsg isolateMsg, final String resourcePathPrefix) throws IOException {
        if (isolateMsg == null) {
            return;
        }
        // update order with yaml data
        ObjectReader orderReader = EmapYamlMapper.readerForUpdating(isolateMsg);
        String isolateDefaultPath = resourcePathPrefix + "_isolate_defaults.yaml";
        isolateMsg = orderReader.readValue(getInputStream(isolateDefaultPath));
        updateLabResults(isolateMsg.getSensitivities(), resourcePathPrefix);
    }

    public LabOrderMsg buildLabOrderOverridingDefaults(String defaultsFile, String overridingFile) throws IOException {
        String defaultsPath = String.format("/LabOrders/%s", defaultsFile);
        String overridingPath = String.format("/LabOrders/%s", overridingFile);
        InputStream inputStream = getInputStream(defaultsPath);
        LabOrderMsg defaults = EmapYamlMapper.readValue(inputStream, new TypeReference<>() {});
        ObjectReader orderReader = EmapYamlMapper.readerForUpdating(defaults);

        return orderReader.readValue(getInputStream(overridingPath));
    }

    /**
     * Load test form data.
     * @param fileName message array json file
     * @return list of FormMsg from this file
     * @throws IOException if file cannot be read
     */
    public List<FormMsg> getFormMsgs(final String fileName) throws IOException {
        String resourcePath = "/Form/" + fileName;
        InputStream inputStream = getInputStream(resourcePath);
        List<FormMsg> formMsgs = EmapYamlMapper.readValue(inputStream, new TypeReference<List<FormMsg>>() {});
        for (FormMsg msg : formMsgs) {
            // These source IDs are derived, so generate programmatically here.
            // All answers should have the same filing time, perhaps an argument for moving this to the form?
            // A form without any answers is invalid.
            Instant answersFiledDatetime = msg.getFormAnswerMsgs().get(0).getFiledDatetime();
            msg.setSourceMessageId(String.format("%s_%s_%s", msg.getFirstFiledDatetime(), answersFiledDatetime, msg.getFormInstanceId()));
        }
        return formMsgs;
    }

    /**
     * Load test form metadata. As in the real life data, the "valid from" date for metadata is sometimes not known,
     * and the expected behaviour in this case is to use the current time. The fallbackValidFrom date gives the test
     * a mechanism to say what the correct "now" timestamp is, otherwise you'd have to do some fudging in the assertion:
     * The expected "now" would usually be a few milliseconds after the actual now, unless your computer
     * was unexpectedly slow, or you set a breakpoint, etc. Better to be exact than go for any weird heuristics.
     * @param fileName message array json file
     * @param fallbackValidFrom Timestamp to fill in if the validFrom is null
     * @return list of FormMetadataMsg from this file
     * @throws IOException if file cannot be read
     */
    public List<FormMetadataMsg> getFormMetadataMsg(final String fileName, Instant fallbackValidFrom) throws IOException {
        String resourcePath = "/Form/" + fileName;
        InputStream inputStream = getInputStream(resourcePath);
        List<FormMetadataMsg> formMetadataMsgs = EmapYamlMapper.readValue(inputStream, new TypeReference<List<FormMetadataMsg>>() {});
        for (var m : formMetadataMsgs) {
            // if validFrom has not been set then fill it in from the fallback value, if set
            if (fallbackValidFrom != null && m.getValidFrom() == null) {
                m.setValidFrom(fallbackValidFrom);
            }
        }
        return formMetadataMsgs;
    }

    /**
     * Load test form question metadata. fallbackValidFrom exists for same reason as in {@link #getFormMetadataMsg}
     * @param fileName expected messages yaml file to read from
     * @param fallbackValidFrom Timestamp to fill in if the validFrom is null
     * @return list of FormQuestionMetadataMsg from this file
     * @throws IOException if file cannot be read
     */
    public List<FormQuestionMetadataMsg> getFormQuestionMetadataMsg(final String fileName, Instant fallbackValidFrom) throws IOException {
        String resourcePath = "/Form/" + fileName;
        InputStream inputStream = getInputStream(resourcePath);
        List<FormQuestionMetadataMsg> formMsgs = EmapYamlMapper.readValue(inputStream, new TypeReference<List<FormQuestionMetadataMsg>>() {});
        for (var m : formMsgs) {
            // if validFrom has not been set then fill it in from the fallback value, if set
            if (fallbackValidFrom != null && m.getValidFrom() == null) {
                m.setValidFrom(fallbackValidFrom);
            }
        }
        return formMsgs;
    }

    /**
     * Get the input stream for a path while monitoring the access if required
     * @param path path as a string
     * @return Input stream
     * @throws IOException if the file does not exist in the resource file store
     */
    InputStream getInputStream(String path) throws IOException {

        if (fileStore == null){
            return getClass().getResourceAsStream(path);
        }

        return getClass().getResourceAsStream(fileStore.get(path));
    }

    public void updateFileStoreWith(Class<?> rootClass) throws URISyntaxException, IOException {
        fileStore.updateFilesFromClassResources(rootClass);
    }

    public FileStoreWithMonitoredAccess getFileStore(){
        return fileStore;
    }
}
