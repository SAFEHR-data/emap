package uk.ac.ucl.rits.inform.datasources.ids;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.test.context.ActiveProfiles;
import uk.ac.ucl.rits.inform.interchange.AdvanceDecisionMessage;
import uk.ac.ucl.rits.inform.interchange.ConsultRequest;
import uk.ac.ucl.rits.inform.interchange.EmapOperationMessage;
import uk.ac.ucl.rits.inform.interchange.InterchangeValue;
import uk.ac.ucl.rits.inform.interchange.PatientAllergy;
import uk.ac.ucl.rits.inform.interchange.PatientInfection;
import uk.ac.ucl.rits.inform.interchange.PatientProblem;
import uk.ac.ucl.rits.inform.interchange.adt.AdtMessage;
import uk.ac.ucl.rits.inform.interchange.adt.ImpliedAdtMessage;
import uk.ac.ucl.rits.inform.interchange.lab.LabOrderMsg;
import uk.ac.ucl.rits.inform.interchange.lab.LabResultMsg;
import uk.ac.ucl.rits.inform.interchange.test.helpers.EmapYamlMapper;
import uk.ac.ucl.rits.inform.interchange.test.helpers.InterchangeMessageFactory;
import uk.ac.ucl.rits.inform.interchange.visit_observations.Flowsheet;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.StringJoiner;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;


/**
 * Test that the HL7 output format matches that of the corresponding yaml files
 */
@ActiveProfiles("test")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Slf4j
public class TestHL7ParsingMatchesInterchangeFactoryOutput extends TestHl7MessageStream {
    InterchangeMessageFactory interchangeFactory;


    /**
     * Constructor for the test class. Populates all the message files
     * @throws IOException If a path cannot be accessed
     */
    TestHL7ParsingMatchesInterchangeFactoryOutput() throws IOException, URISyntaxException {
        interchangeFactory = InterchangeMessageFactory.withMonitoredFiles();
        interchangeFactory.updateFileStoreWith(getClass());
    }

    private static void assertEquals(EmapOperationMessage expected, EmapOperationMessage actual, String message) {
        Assertions.assertEquals(EmapYamlMapper.convertToString(expected), EmapYamlMapper.convertToString(actual), message);
    }

    private static void assertEquals(EmapOperationMessage expected, EmapOperationMessage actual) {
        Assertions.assertEquals(EmapYamlMapper.convertToString(expected), EmapYamlMapper.convertToString(actual));
    }

    private void testAdtMessage(String adtFileStem) throws Exception {
        log.info("Testing ADT message with stem '{}'", adtFileStem);
        List<? extends EmapOperationMessage> messagesFromHl7Message = processSingleMessage("Adt/" + adtFileStem + ".txt");
        AdtMessage expectedAdtMessage = interchangeFactory.getAdtMessage(adtFileStem + ".yaml");
        Assertions.assertEquals(1, messagesFromHl7Message.size());
        assertEquals(expectedAdtMessage, messagesFromHl7Message.get(0));
    }

    private void assertListOfMessagesEqual(List<? extends EmapOperationMessage> expectedMessages, List<? extends EmapOperationMessage> messagesFromHl7Message) {
        for (int i = 0; i < expectedMessages.size(); i++) {
            String failMessage = String.format("Failed on message %d", i);
            assertEquals(expectedMessages.get(i), messagesFromHl7Message.get(i), failMessage);
        }
        Assertions.assertEquals(expectedMessages.size(), messagesFromHl7Message.size());
    }

    private void assertLabOrdersWithValueAsBytesEqual(List<LabOrderMsg> expectedMessages, List<? extends EmapOperationMessage> messagesFromHl7Message) {
        // first check values as byte and strip them out
        for (int orderIndex = 0; orderIndex < expectedMessages.size(); orderIndex++) {
            if (expectedMessages.get(orderIndex).getLabResultMsgs().isEmpty()) {
                continue;
            }
            LabOrderMsg expectedOrder = expectedMessages.get(orderIndex);
            LabOrderMsg hl7Order = (LabOrderMsg) messagesFromHl7Message.get(orderIndex);
            for (int resultIndex = 0; resultIndex < expectedOrder.getLabResultMsgs().size(); resultIndex++) {
                LabResultMsg expectedResult = expectedOrder.getLabResultMsgs().get(resultIndex);
                LabResultMsg hl7Result = hl7Order.getLabResultMsgs().get(resultIndex);
                if (expectedResult.getByteValue().isUnknown()) {
                    continue;
                }
                // check byte values
                byte[] expectedBytes = expectedResult.getByteValue().get();
                byte[] hl7Bytes = hl7Result.getByteValue().get();
                assertArrayEquals(expectedBytes, hl7Bytes);
                // remove byte values from rest of the check
                expectedResult.setByteValue(InterchangeValue.unknown());
                hl7Result.setByteValue(InterchangeValue.unknown());
            }
        }
        assertListOfMessagesEqual(expectedMessages, messagesFromHl7Message);
    }

    @Test
    public void testGenericAdtA01() throws Exception {
        testAdtMessage("generic/A01");
    }

    @Test
    public void testGenericAdtA02() throws Exception {
        testAdtMessage("generic/A02");
    }

    @Test
    public void testGenericAdtA03() throws Exception {
        testAdtMessage("generic/A03");
    }

    @Test
    public void testGenericAdtA04() throws Exception {
        testAdtMessage("generic/A04");
    }

    @Test
    public void testGenericAdtA06() throws Exception {
        testAdtMessage("generic/A06");
    }

    @Test
    public void testGenericAdtA08() throws Exception {
        testAdtMessage("generic/A08_v1");
    }

    @Test
    public void testGenericAdtA11() throws Exception {
        testAdtMessage("generic/A11");
    }

    @Test
    public void testGenericAdtA12() throws Exception {
        testAdtMessage("generic/A12");
    }

    @Test
    public void testGenericAdtA13() throws Exception {
        testAdtMessage("generic/A13");
    }

    @Test
    public void testGenericAdtA17() throws Exception {
        testAdtMessage("generic/A17");
    }

    @Test
    public void testGenericAdtA29() throws Exception {
        testAdtMessage("generic/A29");
    }

    @Test
    public void testGenericAdtA40() throws Exception {
        testAdtMessage("generic/A40");
    }

    @Test
    public void testGenericAdtA45() throws Exception {
        testAdtMessage("generic/A45");
    }

    @Test
    public void testGenericAdtA47() throws Exception {
        testAdtMessage("generic/A47");
    }

    @Test
    public void testPendingAdt() throws Exception {
        testAdtMessage("pending/A15");
        testAdtMessage("pending/A26");
    }

    @Test
    public void testDoubleA01WithA13() throws Exception {
        testAdtMessage("DoubleA01WithA13/A03");
        testAdtMessage("DoubleA01WithA13/A08");
        testAdtMessage("DoubleA01WithA13/A13");
    }

    @Test
    void testAdtPermutationMoves() throws Exception {
        String[] fileNames = {"02_A01", "03_A02", "04_A02", "05_A02", "06_A02", "07_A06", "08_A03", "09_Z99"};

        builtAndAssertAdtMessages("Adt", "Location/Moves", fileNames);
    }

    private void builtAndAssertAdtMessages
            (String hl7PathBase, String sharedBase, String[] fileNames) throws Exception {
        String hl7PathTemplate = String.join("/", hl7PathBase, sharedBase, "%s.txt");
        String interchangePathTemplate = String.join("/", sharedBase, "%s.yaml");
        Collection<EmapOperationMessage> builtMessages = new ArrayList<>();
        Collection<EmapOperationMessage> expectedMessages = new ArrayList<>();
        for (String fileName : fileNames) {
            log.info("Processing file {}", fileName);
            EmapOperationMessage builtMessage = processSingleMessage(String.format(hl7PathTemplate, fileName)).stream().findFirst().orElseThrow();
            EmapOperationMessage expectedMessage = interchangeFactory.getAdtMessage(String.format(interchangePathTemplate, fileName));

            assertEquals(expectedMessage, builtMessage);
            expectedMessages.add(expectedMessage);
            builtMessages.add(builtMessage);
        }

        Assertions.assertEquals(expectedMessages.size(), builtMessages.size());
    }

    @Test
    void testPermutationCancelAdmit() throws Exception {
        String[] fileNames = {"01_A01", "02_A11", "03_A01", "04_A02", "05_A03"};

        builtAndAssertAdtMessages("Adt", "Location/CancelAdmit", fileNames);
    }

    @Test
    void testPermutationCancelDischarge() throws Exception {
        String[] fileNames = {"01_A01", "02_A02", "03_A03", "04_A13", "05_A03"};

        builtAndAssertAdtMessages("Adt", "Location/CancelDischarge", fileNames);
    }

    @Test
    void testPermutationCancelTransfer() throws Exception {
        String[] fileNames = {"01_A01", "02_A02", "03_A02", "04_A12", "05_A02", "06_A03"};

        builtAndAssertAdtMessages("Adt", "Location/CancelTransfer", fileNames);
    }

    @Test
    void testPermutationDuplicateSimple() throws Exception {
        String[] fileNames = {"01_A04", "02_A01", "03_A02", "04_A03"};

        builtAndAssertAdtMessages("Adt", "Location/DuplicateSimple", fileNames);
    }

    void checkConsultMatchesInterchange(String fileName) throws Exception {
        List<? extends EmapOperationMessage> messagesFromHl7Message = processSingleMessageAndRemoveAdt("ConsultRequest/" + fileName + ".txt");
        ConsultRequest expected = interchangeFactory.getConsult(String.format("%s.yaml", fileName));
        Assertions.assertEquals(1, messagesFromHl7Message.size());
        assertEquals(expected, messagesFromHl7Message.get(0));
    }

    @Test
    void testClosedAtDischarge() throws Exception {
        checkConsultMatchesInterchange("closed_at_discharge");
    }

    @Test
    void testCancelledConsult() throws Exception {
        checkConsultMatchesInterchange("cancelled");
    }

    @Test
    void testMinimalConsult() throws Exception {
        checkConsultMatchesInterchange("minimal");
    }

    @Test
    void testNotesConsult() throws Exception {
        checkConsultMatchesInterchange("notes");
    }

    void checkAdvanceDecisionMatchesInterchange(String fileName) throws Exception {
        checkAdvanceDecisionMatchesInterchange(String.format("AdvanceDecision/%s.txt", fileName), String.format("%s.yaml", fileName));
    }

    void checkAdvanceDecisionMatchesInterchange(String txtFileName, String yamlFileName) throws Exception {

        List<? extends EmapOperationMessage> messagesFromHl7Message = processSingleMessageAndRemoveAdt(txtFileName);
        AdvanceDecisionMessage expected = interchangeFactory.getAdvanceDecision(yamlFileName);
        Assertions.assertEquals(1, messagesFromHl7Message.size());
        assertEquals(expected, messagesFromHl7Message.get(0));
    }

    @Test
    void testClosedAtDischargeAdvanceDecision() throws Exception {
        checkAdvanceDecisionMatchesInterchange("closed_at_discharge");
    }

    @Test
    void testCancelledAdvanceDecision() throws Exception {
        checkAdvanceDecisionMatchesInterchange("cancelled");
    }

    @Test
    void testMinimalAdvanceDecision() throws Exception {
        checkAdvanceDecisionMatchesInterchange("minimal");
    }

    @Test
    void testMinimalWithQuestionsAdvanceDecision() throws Exception {
        checkAdvanceDecisionMatchesInterchange("new_with_questions");
    }

    @Test
    public void testLabIncrementalLoad() throws Exception {
        List<? extends EmapOperationMessage> messagesFromHl7Message = processLabHl7AndFilterToLabOrderMsgs(
                "LabOrders/winpath/Incremental.txt");
        List<LabOrderMsg> expectedOrders = interchangeFactory.getLabOrders("winpath/incremental.yaml");
        assertListOfMessagesEqual(expectedOrders, messagesFromHl7Message);
    }

    @Test
    public void testLabOrderMsg() throws Exception {
        List<? extends EmapOperationMessage> messagesFromHl7Message = processSingleMessageAndRemoveAdt(
                "LabOrders/winpath/ORU_R01.txt");
        List<LabOrderMsg> expectedOrders = interchangeFactory.getLabOrders("winpath/ORU_R01.yaml");
        assertListOfMessagesEqual(expectedOrders, messagesFromHl7Message);
    }

    @Test
    public void testLabSensitivity() throws Exception {
        List<? extends EmapOperationMessage> messagesFromHl7Message = processSingleMessageAndRemoveAdt(
                "LabOrders/winpath/Sensitivity.txt");
        List<LabOrderMsg> expectedOrders = interchangeFactory.getLabOrders("winpath/sensitivity.yaml");
        assertListOfMessagesEqual(expectedOrders, messagesFromHl7Message);
    }

    @Test
    public void testIncrementalIsolate1() throws Exception {
        List<? extends EmapOperationMessage> messagesFromHl7Message = processSingleMessageAndRemoveAdt(
                "LabOrders/winpath/isolate_inc_1.txt");
        List<LabOrderMsg> expectedOrders = interchangeFactory.getLabOrders("winpath/isolate_inc_1.yaml");
        assertListOfMessagesEqual(expectedOrders, messagesFromHl7Message);
    }

    @Test
    public void testIncrementalIsolate2() throws Exception {
        List<? extends EmapOperationMessage> messagesFromHl7Message = processSingleMessageAndRemoveAdt(
                "LabOrders/winpath/isolate_inc_2.txt");
        List<LabOrderMsg> expectedOrders = interchangeFactory.getLabOrders("winpath/isolate_inc_2.yaml");
        assertListOfMessagesEqual(expectedOrders, messagesFromHl7Message);
    }

    @Test
    public void testWinPathIncrementalOrders() throws Exception {
        String hl7PathTemplate = "LabOrders/winpath/incremental_orders/%s.txt";
        String interchangePathTemplate = "winpath/incremental_orders/%s.yaml";
        String interchangeDefaults = String.format(interchangePathTemplate, "orm_defaults");

        List<EmapOperationMessage> builtMessages = new ArrayList<>();
        List<LabOrderMsg> expectedOrders = new ArrayList<>();
        // build up order messages
        String[] orderFiles = {"01_orm_o01_nw", "02_orm_o01_sc_mg", "03_orm_o01_sn_telh", "04_orr_o02_telh"};
        for (String orderFile : orderFiles) {
            builtMessages.addAll(processSingleMessage(String.format(hl7PathTemplate, orderFile)));
            expectedOrders.add(interchangeFactory.buildLabOrderOverridingDefaults(
                    interchangeDefaults, String.format(interchangePathTemplate, orderFile)));
        }
        // add in final result
        builtMessages.addAll(processSingleMessage(String.format(hl7PathTemplate, "05_oru_r01")));
        expectedOrders.addAll(interchangeFactory.getLabOrders(String.format(interchangePathTemplate, "05_oru_r01")));

        builtMessages = builtMessages.stream().filter(msg -> !(msg instanceof ImpliedAdtMessage)).collect(Collectors.toList());
        assertListOfMessagesEqual(expectedOrders, builtMessages);
    }

    @Test
    public void testWinPathCancelOrders() throws Exception {
        String hl7PathTemplate = "LabOrders/winpath/cancel_orders/%s.txt";
        String interchangePathTemplate = "winpath/cancel_orders/%s.yaml";
        String interchangeDefaults = String.format(interchangePathTemplate, "orm_defaults");

        List<EmapOperationMessage> builtMessages = new ArrayList<>();
        List<LabOrderMsg> expectedOrders = new ArrayList<>();
        // build up order messages
        String[] orderFiles = {"01_orm_o01_nw_fbc_mg", "02_orm_o01_ca_fbc", "03_orm_o01_sn_fbcc", "04_orr_o02_cr_fbc", "05_orr_o02_na_fbcc"};
        for (String orderFile : orderFiles) {
            builtMessages.addAll(processSingleMessage(String.format(hl7PathTemplate, orderFile)));
            expectedOrders.add(interchangeFactory.buildLabOrderOverridingDefaults(
                    interchangeDefaults, String.format(interchangePathTemplate, orderFile)));
        }
        // add in final result
        builtMessages.addAll(processSingleMessage(String.format(hl7PathTemplate, "06_oru_r01_fbcc")));
        expectedOrders.addAll(interchangeFactory.getLabOrders(String.format(interchangePathTemplate, "06_oru_r01_fbcc")));

        builtMessages = builtMessages.stream().filter(msg -> !(msg instanceof ImpliedAdtMessage)).collect(Collectors.toList());
        assertListOfMessagesEqual(expectedOrders, builtMessages);
    }

    @Test
    public void testCoPathIncrementalOrder() throws Exception {
        String hl7PathTemplate = "LabOrders/co_path/incremental/%s.txt";
        String interchangePathTemplate = "co_path/incremental/%s.yaml";
        String interchangeDefaults = String.format(interchangePathTemplate, "orm_defaults");

        List<EmapOperationMessage> builtMessages = new ArrayList<>();
        List<LabOrderMsg> expectedOrders = new ArrayList<>();
        // build up order messages
        String[] orderFiles = {"01_orm_o01_sn", "02_orm_o01_nw", "03_orr_o02_na"};
        for (String orderFile : orderFiles) {
            builtMessages.addAll(processSingleMessage(String.format(hl7PathTemplate, orderFile)));
            expectedOrders.add(interchangeFactory.buildLabOrderOverridingDefaults(
                    interchangeDefaults, String.format(interchangePathTemplate, orderFile)));
        }
        // add in final result
        builtMessages.addAll(processSingleMessage(String.format(hl7PathTemplate, "04_oru_r01")));
        expectedOrders.addAll(interchangeFactory.getLabOrders(String.format(interchangePathTemplate, "04_oru_r01")));

        builtMessages = builtMessages.stream().filter(msg -> !(msg instanceof ImpliedAdtMessage)).collect(Collectors.toList());
        assertLabOrdersWithValueAsBytesEqual(expectedOrders, builtMessages);
    }

    @Test
    public void testCoPathCancelOrders() throws Exception {
        String hl7PathTemplate = "LabOrders/co_path/cancel/%s.txt";
        String interchangePathTemplate = "co_path/cancel/%s.yaml";
        String interchangeDefaults = String.format(interchangePathTemplate, "orm_defaults");

        List<EmapOperationMessage> builtMessages = new ArrayList<>();
        List<LabOrderMsg> expectedOrders = new ArrayList<>();
        // build up order messages
        String[] orderFiles = {"01_orm_o01_nw", "02_orm_o01_ca", "03_orr_o02_cr", "04_orm_o01_sc"};
        for (String orderFile : orderFiles) {
            builtMessages.addAll(processSingleMessage(String.format(hl7PathTemplate, orderFile)));
            expectedOrders.add(interchangeFactory.buildLabOrderOverridingDefaults(
                    interchangeDefaults, String.format(interchangePathTemplate, orderFile)));
        }
        // add in final result
        builtMessages.addAll(processSingleMessage(String.format(hl7PathTemplate, "05_oru_r01")));
        expectedOrders.addAll(interchangeFactory.getLabOrders(String.format(interchangePathTemplate, "05_oru_r01")));

        builtMessages = builtMessages.stream().filter(msg -> !(msg instanceof ImpliedAdtMessage)).collect(Collectors.toList());
        assertListOfMessagesEqual(expectedOrders, builtMessages);
    }

    @Test
    void testCoPathQuestions() throws Exception {
        String hl7PathTemplate = "LabOrders/co_path/%s.txt";
        String interchangePathTemplate = "co_path/%s.yaml";
        String orderFile = "orm_o01_questions";
        String interchangeDefaults = String.format(interchangePathTemplate, "orm_defaults");
        String interchangePath = String.format(interchangePathTemplate, orderFile);

        EmapOperationMessage builtMessage = processSingleMessage(String.format(hl7PathTemplate, orderFile))
                .stream()
                .filter(msg -> !(msg instanceof ImpliedAdtMessage))
                .findFirst().orElseThrow();
        LabOrderMsg expectedMessage = interchangeFactory.buildLabOrderOverridingDefaults(interchangeDefaults, interchangePath);

        assertEquals(builtMessage, expectedMessage);
    }

    @Test
    void testCoPathByteValue() throws Exception {
        String hl7PathTemplate = "LabOrders/co_path/%s.txt";
        String interchangePathTemplate = "co_path/%s.yaml";
        String orderFile = "oru_r01_byte_value";

        LabOrderMsg builtMessage = (LabOrderMsg) processSingleMessage(String.format(hl7PathTemplate, orderFile))
                .stream()
                .filter(msg -> (msg instanceof LabOrderMsg))
                .findFirst().orElseThrow();
        LabOrderMsg expectedMessage = interchangeFactory.getLabOrder(
                String.format(interchangePathTemplate, orderFile));

        assertLabOrdersWithValueAsBytesEqual(List.of(expectedMessage), List.of(builtMessage));
    }

    @Test
    void testImagingLabs() throws Exception {
        String hl7PathTemplate = "LabOrders/imaging/%s.txt";
        String interchangePathTemplate = "imaging/%s.yaml";
        String orderFile = "oru_r01_imaging_result";
        LabOrderMsg builtMessage = (LabOrderMsg) processSingleMessage(String.format(hl7PathTemplate, orderFile))
                .stream()
                .filter(msg -> (msg instanceof LabOrderMsg))
                .findFirst().orElseThrow();
        LabOrderMsg expectedMessage = interchangeFactory.getLabOrder(String.format(interchangePathTemplate, orderFile));

        assertEquals(expectedMessage, builtMessage);
    }

    @Test
    public void testVitalSigns() throws Exception {
        List<? extends EmapOperationMessage> messagesFromHl7Message = processSingleMessageAndRemoveAdt("VitalSigns/MixedHL7Message.txt");
        List<Flowsheet> expectedOrders = interchangeFactory.getFlowsheets("hl7.yaml");
        assertListOfMessagesEqual(expectedOrders, messagesFromHl7Message);
    }

    public void checkPatientInfectionMatchesInterchange(String txtFileName, String yamlFileName) throws Exception {
        List<EmapOperationMessage> messagesFromHl7 = processSingleMessage(txtFileName)
                .stream()
                .filter(msg -> msg instanceof PatientInfection).collect(Collectors.toList());

        List<PatientInfection> expectedMessages = interchangeFactory.getPatientInfections(yamlFileName);

        for (int i = 0; i < expectedMessages.size(); i++) {
            assertEquals(expectedMessages.get(i), messagesFromHl7.get(i));
        }

        Assertions.assertEquals(expectedMessages.size(), messagesFromHl7.size());
    }

    @Test
    public void testMinimalPatientInfection() throws Exception {
        checkPatientInfectionMatchesInterchange("PatientInfection/a05.txt", "hl7/minimal_mumps.yaml");
    }

    @Test
    void testPatientInfection() throws Exception {
        var messageFromHl7 = processSingleMessageFirstOfType("PatientInfection/a05.txt", PatientInfection.class);
        PatientInfection expected = interchangeFactory.getPatientInfections("hl7/minimal_mumps.yaml").get(0);
        assertEquals(expected, messageFromHl7);
    }

    @Test
    public void testPatientAllergy() throws Exception {
        var messageFromHl7 = processSingleMessageFirstOfType("PatientAllergies/minimal_allergy.txt", PatientAllergy.class);
        PatientAllergy expected = interchangeFactory.getPatientAllergies("hl7/minimal_allergy.yaml").get(0);
        assertEquals(expected, messageFromHl7);
    }

    @Test
    void testPatientProblem() throws Exception {
        String[] fileNames = {
                "minimal_myeloma_outpatient", "minimal_myeloma_inpatient", "myeloma_add", "minimal_other_problem_inpatient"
        };
        for (String fileName : fileNames) {
            log.info("Testing file {}", fileName);
            String hl7FileName = String.format("ProblemList/end_to_end/%s.txt", fileName);
            EmapOperationMessage messageFromHl7 = processSingleMessageFirstOfType(hl7FileName, PatientProblem.class);
            String interchangeFileName = String.format("hl7/%s.yaml", fileName);
            PatientProblem expected = interchangeFactory.getPatientProblems(interchangeFileName).stream().findFirst().orElseThrow();
            assertEquals(expected, messageFromHl7);
        }
    }

    /**
     * Ensure that all the interchange yaml files created from hl7 messages (i.e. those with an "EPIC" source system
     * and potentially others) have been accessed, thus have been checked against their yaml counterparts.
     * @throws Exception If not all the files have been accessed
     */
    @AfterAll
    void checkAllFilesHaveBeenAccessed() throws Exception {

        var excludedSourceSystems = Set.of("clarity", "caboodle", "ids");
        var missedFilePaths = new ArrayList<String>();

        for (var file : interchangeFactory.getFileStore()) {

            if (!file.getFilePathString().endsWith(".yaml")
                    || file.getFilePathString().endsWith("_defaults.yaml") // Implicitly considered - non-prefixed version inherits
                    || file.hasBeenAccessed()
                    || file.sourceSystem().isEmpty()) {
                continue;
            }

            if (excludedSourceSystems.contains(file.sourceSystem().get())) {
                continue;  // Source system is excluded
            }

            missedFilePaths.add(file.getFilePathString());
        }

        if (!missedFilePaths.isEmpty()) {
            String sortedAndJoined = missedFilePaths.stream().sorted().collect(Collectors.joining("\n\t"));
            String message = new StringJoiner("\n")
                    .add("Not all interchange YAML files have been accessed.")
                    .add("Make sure that you are using the `getInputStream` method in the interchange factory.")
                    .add("Otherwise it looks like you've defined data that is not from caboodle or clarity,")
                    .add("we should test that we can build an hl7 message that matches the yaml file in this class.")
                    .add("Missing files:")
                    .add(String.format("\t%s", sortedAndJoined))
                    .toString();

            throw new Exception(message);
        }
    }
}
