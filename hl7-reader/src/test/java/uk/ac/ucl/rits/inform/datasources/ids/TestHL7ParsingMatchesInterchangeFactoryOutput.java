package uk.ac.ucl.rits.inform.datasources.ids;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.TestInstance;
import org.springframework.test.context.ActiveProfiles;

import uk.ac.ucl.rits.inform.interchange.*;
import uk.ac.ucl.rits.inform.interchange.adt.AdtMessage;
import uk.ac.ucl.rits.inform.interchange.adt.ImpliedAdtMessage;
import uk.ac.ucl.rits.inform.interchange.lab.LabOrderMsg;
import uk.ac.ucl.rits.inform.interchange.lab.LabResultMsg;
import uk.ac.ucl.rits.inform.interchange.visit_observations.Flowsheet;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;




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
        interchangeFactory = new InterchangeMessageFactory();
        interchangeFactory.fileStore.updateResourceFileFromClass(getClass());
    }


    private void testAdtMessage(String adtFileStem) throws Exception {
        log.info("Testing ADT message with stem '{}'", adtFileStem);
        System.out.println(adtFileStem);
        List<? extends EmapOperationMessage> messagesFromHl7Message = processSingleMessage("Adt/" + adtFileStem + ".txt");
        AdtMessage expectedAdtMessage = interchangeFactory.getAdtMessage(adtFileStem + ".yaml");
        Assertions.assertEquals(1, messagesFromHl7Message.size());
        Assertions.assertEquals(expectedAdtMessage, messagesFromHl7Message.get(0));
    }

    private void assertListOfMessagesEqual(List<? extends EmapOperationMessage> expectedMessages, List<? extends EmapOperationMessage> messagesFromHl7Message) {
        for (int i = 0; i < expectedMessages.size(); i++) {
            String failMessage = String.format("Failed on message %d", i);
            Assertions.assertEquals(expectedMessages.get(i), messagesFromHl7Message.get(i), failMessage);
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
        testAdtMessage("generic/A01_b");

    }

    @Test
    public void testGenericAdtA02() throws Exception {
        testAdtMessage("generic/A02");
    }

    @Test
    public void testGenericAdtA03() throws Exception {
        testAdtMessage("generic/A03");
        testAdtMessage("generic/A03_death");
        testAdtMessage("generic/A03_death_2");
        testAdtMessage("generic/A03_death_3");

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
        testAdtMessage("generic/A08_v2");
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
    void testPendingTransferAdtA15() throws Exception {
        testAdtMessage("pending/A15");
    }

    @Test
    public void testGenericAdtA17() throws Exception {
        testAdtMessage("generic/A17");
    }

    @Test
    void testCancelPendingTransferA26() throws Exception {
        testAdtMessage("pending/A26");
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
    public void testDoubleA01WithA13() throws Exception {
        testAdtMessage("DoubleA01WithA13/A03");
        testAdtMessage("DoubleA01WithA13/A03_2");
        testAdtMessage("DoubleA01WithA13/A08");
        testAdtMessage("DoubleA01WithA13/A13");
        testAdtMessage("DoubleA01WithA13/FirstA01");
        testAdtMessage("DoubleA01WithA13/SecondA01");
    }

    @Test
    public void testAdtPendingLocations() throws Exception {
        testAdtMessage("pending/A15");
        testAdtMessage("pending/A15_null_pending_location");
        testAdtMessage("pending/A26");
    }

    void checkConsultMatchesInterchange(String fileName) throws Exception {
        List<? extends EmapOperationMessage> messagesFromHl7Message = processSingleMessageAndRemoveAdt("ConsultRequest/"+fileName+".txt");
        ConsultRequest expected = interchangeFactory.getConsult(String.format("%s.yaml", fileName));
        assertEquals(1, messagesFromHl7Message.size());
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
        checkAdvanceDecisionMatchesInterchange("AdvanceDecision/"+fileName+".txt",
                String.format("%s.yaml", fileName));
    }

    void checkAdvanceDecisionMatchesInterchange(String txtFileName, String yamlFileName) throws Exception{

        List<? extends EmapOperationMessage> messagesFromHl7Message = processSingleMessageAndRemoveAdt(txtFileName);
        AdvanceDecisionMessage expected = interchangeFactory.getAdvanceDecision(yamlFileName);
        assertEquals(1, messagesFromHl7Message.size());
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
    void testMinimalWithMultipleQuestionsAdvanceDecision() throws Exception {
        checkAdvanceDecisionMatchesInterchange("minimal_w_questions");
    }

    @Test
    public void testLabIncrementalLoad() throws Exception {
        List<? extends EmapOperationMessage> messagesFromHl7Message = processLabHl7AndFilterToLabOrderMsgs(
                "LabOrders/winpath/Incremental.txt");
        List<LabOrderMsg> expectedOrders = interchangeFactory.getLabOrders("winpath/incremental.yaml", "0000000042");
        assertListOfMessagesEqual(expectedOrders, messagesFromHl7Message);
    }

    @Test
    public void testLabIncrementalDuplicateResultSegment() throws Exception {
        List<? extends EmapOperationMessage> messagesFromHl7Message = processLabHl7AndFilterToLabOrderMsgs(
                "LabOrders/winpath/LabDuplicateResultSegment.txt");
        List<LabOrderMsg> expectedOrders = interchangeFactory.getLabOrders(
                "winpath/incremental_duplicate_result_segment.yaml", "0000000042");
        assertListOfMessagesEqual(expectedOrders, messagesFromHl7Message);
    }

    @Test
    public void testLabOrderMsg() throws Exception {
        List<? extends EmapOperationMessage> messagesFromHl7Message = processSingleMessageAndRemoveAdt(
                "LabOrders/winpath/ORU_R01.txt");
        List<LabOrderMsg> expectedOrders = interchangeFactory.getLabOrders("winpath/ORU_R01.yaml", "0000000042");
        assertListOfMessagesEqual(expectedOrders, messagesFromHl7Message);
    }

    @Test
    public void testLabOrderMsgProducesAdtFirst() throws Exception {
        EmapOperationMessage messageFromHl7 = processSingleMessage(
                "LabOrders/winpath/ORU_R01.txt").get(0);
        AdtMessage expectedAdt = interchangeFactory.getAdtMessage(
                "FromNonAdt/lab_oru_r01.yaml");
        Assertions.assertEquals(expectedAdt, messageFromHl7);
    }

    @Test
    public void testLabSensitivity() throws Exception {
        List<? extends EmapOperationMessage> messagesFromHl7Message = processSingleMessageAndRemoveAdt(
                "LabOrders/winpath/Sensitivity.txt");
        List<LabOrderMsg> expectedOrders = interchangeFactory.getLabOrders(
                "winpath/sensitivity.yaml", "0000000042");
        assertListOfMessagesEqual(expectedOrders, messagesFromHl7Message);
    }

    @Test
    public void testIncrementalIsolate1() throws Exception {
        List<? extends EmapOperationMessage> messagesFromHl7Message = processSingleMessageAndRemoveAdt(
                "LabOrders/winpath/isolate_inc_1.txt");
        List<LabOrderMsg> expectedOrders = interchangeFactory.getLabOrders(
                "winpath/isolate_inc_1.yaml", "0000000042");
        assertListOfMessagesEqual(expectedOrders, messagesFromHl7Message);
    }

    @Test
    public void testIncrementalIsolate2() throws Exception {
        List<? extends EmapOperationMessage> messagesFromHl7Message = processSingleMessageAndRemoveAdt(
                "LabOrders/winpath/isolate_inc_2.txt");
        List<LabOrderMsg> expectedOrders = interchangeFactory.getLabOrders(
                "winpath/isolate_inc_2.yaml", "0000000042");
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
        expectedOrders.addAll(interchangeFactory.getLabOrders(
                String.format(interchangePathTemplate, "05_oru_r01"), "0000000042"));

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
        expectedOrders.addAll(interchangeFactory.getLabOrders(
                String.format(interchangePathTemplate, "06_oru_r01_fbcc"), "0000000042"));

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
        expectedOrders.addAll(interchangeFactory.getLabOrders(
                String.format(interchangePathTemplate, "04_oru_r01"), "0000000042"));

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
        expectedOrders.addAll(interchangeFactory.getLabOrders(
                String.format(interchangePathTemplate, "05_oru_r01"), "0000000042"));

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

        assertLabOrdersWithValueAsBytesEqual(List.of(builtMessage), List.of(expectedMessage));
    }

    @Test
    public void testPOCLabABL() throws Exception {
        List<? extends EmapOperationMessage> messagesFromHl7Message = processSingleMessageAndRemoveAdt(
                "LabOrders/abl90_flex/venous.txt");
        List<LabOrderMsg> expectedOrders = interchangeFactory.getLabOrders(
                "abl90_flex/venous.yaml", "0000000042");
        assertListOfMessagesEqual(expectedOrders, messagesFromHl7Message);
    }

    @Test
    public void testPOCLabBioConnect() throws Exception {
        List<? extends EmapOperationMessage> messagesFromHl7Message = processSingleMessageAndRemoveAdt(
                "LabOrders/bio_connect/glucose.txt");
        List<LabOrderMsg> expectedOrders = interchangeFactory.getLabOrders(
                "bio_connect/glucose.yaml", "0000000042");
        assertListOfMessagesEqual(expectedOrders, messagesFromHl7Message);
    }

    @Test
    public void testVitalSigns() throws Exception {
        List<? extends EmapOperationMessage> messagesFromHl7Message = processSingleMessageAndRemoveAdt("VitalSigns/MixedHL7Message.txt");
        List<Flowsheet> expectedOrders = interchangeFactory.getFlowsheets(
                "hl7.yaml", "0000000042");
        assertListOfMessagesEqual(expectedOrders, messagesFromHl7Message);
    }

    @Test
    public void testVitalSignsProducesAdtFirst() throws Exception {
        EmapOperationMessage messageFromHl7 = processSingleMessage("VitalSigns/MixedHL7Message.txt").get(0);
        AdtMessage expectedAdt = interchangeFactory.getAdtMessage(
                "FromNonAdt/flowsheet_oru_r01.yaml");
        Assertions.assertEquals(expectedAdt, messageFromHl7);
    }

    @Test
    public void testPatientInfectionCreatesAdt() throws Exception {
        EmapOperationMessage messageFromHl7 = processSingleMessage("PatientInfection/a05.txt").get(0);
        AdtMessage expectedAdt = interchangeFactory.getAdtMessage(
                "FromNonAdt/patient_infection_a05.yaml");
        Assertions.assertEquals(expectedAdt, messageFromHl7);
    }

    public void checkPatientInfectionMatchesInterchange(String txtFileName, String yamlFileName) throws Exception {
        List<EmapOperationMessage> messagesFromHl7 = processSingleMessage(txtFileName)
                .stream()
                .filter(msg -> msg instanceof PatientInfection).collect(Collectors.toList());

        List<PatientInfection> expectedMessages = interchangeFactory.getPatientInfections(yamlFileName);

        for (int i = 0; i < Math.max(messagesFromHl7.size(), expectedMessages.size()); i++){
            Assertions.assertEquals(expectedMessages.get(i), messagesFromHl7.get(i));
        }
    }

    @Test
    public void testMinimalPatientInfection() throws Exception {
        checkPatientInfectionMatchesInterchange("PatientInfection/a05.txt", "hl7/minimal_mumps.yaml");
    }

    @Test
    public void testResolvedPatientInfection() throws Exception {
        checkPatientInfectionMatchesInterchange("PatientInfection/mumps_resolved.txt", "mumps_resolved.yaml");
    }

    @Test
    public void testMultiplePatientInfection() throws Exception {
        checkPatientInfectionMatchesInterchange("PatientInfection/multiple_infections.txt",
                "multiple_infections.yaml");
    }

    /**
     * Ensure that all the interchange yaml files created from hl7 messages (i.e. those with an "EPIC" source system)
     * have been accessed, thus have been checked against their yaml counterparts.
     * @throws Exception If not all the files have been accessed
     */
    @AfterAll
    void checkAllFilesHaveBeenAccessed() throws Exception{

        for (var f: interchangeFactory.fileStore){

            if (!f.getFilePath().endsWith(".yaml")
                    || f.getFilePath().endsWith("_defaults.yaml") // Implicitly considered - non-prefixed version inherits
                    || f.hasBeenAccessed()
                    || f.sourceSystem().isEmpty()
                    || (f.sourceSystem().isPresent() && !f.sourceSystem().get().equals("EPIC"))){
                continue;
            }

            throw new Exception("Not all the files have been accessed. Missed "+f.getFilePath());
        }
    }
}
