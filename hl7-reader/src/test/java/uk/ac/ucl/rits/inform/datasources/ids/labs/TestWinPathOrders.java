package uk.ac.ucl.rits.inform.datasources.ids.labs;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import uk.ac.ucl.rits.inform.datasources.ids.exceptions.Hl7InconsistencyException;
import uk.ac.ucl.rits.inform.interchange.InterchangeValue;
import uk.ac.ucl.rits.inform.interchange.OrderCodingSystem;
import uk.ac.ucl.rits.inform.interchange.lab.LabOrderMsg;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test LabOrders derived from Winpath
 * @author Stef Piatek
 */
@ActiveProfiles("test")
@SpringBootTest
class TestWinPathOrders {
    @Autowired
    private LabReader labReader;
    private static final String FILE_TEMPLATE = "LabOrders/winpath/%s.txt";
    private String epicOrder = "91393667";
    private String labOrder = "13U444444";
    private String batteryCode = "MG";
    private Instant collectionTime = Instant.parse("2013-07-28T07:27:00Z");

    @Test
    void testOrmO01NWTimes() throws Exception {
        LabOrderMsg order = labReader.getFirstOrder(FILE_TEMPLATE, "orm_o01_nw");
        assertEquals(collectionTime, order.getCollectionDateTime());
        assertNotNull(order.getStatusChangeTime());
        assertEquals(order.getStatusChangeTime(), order.getOrderDateTime().get());
        assertTrue(order.getRequestedDateTime().isSave());

        assertTrue(order.getSampleReceivedTime().isUnknown());
    }

    @Test
    void testOrmO01NWLabNumbers() throws Exception {
        LabOrderMsg order = labReader.getFirstOrder(FILE_TEMPLATE, "orm_o01_nw");
        assertEquals(InterchangeValue.buildFromHl7(epicOrder), order.getEpicCareOrderNumber());
        assertEquals(labOrder, order.getLabSpecimenNumber());
    }

    @Test
    void testOrmO01NWOrderInfo() throws Exception {
        LabOrderMsg order = labReader.getFirstOrder(FILE_TEMPLATE, "orm_o01_nw");
        assertEquals("Not in Message", order.getSourceSystem());
        assertEquals(InterchangeValue.buildFromHl7("BLD"), order.getSpecimenType());
        assertEquals("Lab", order.getLabDepartment());
        assertEquals(batteryCode, order.getTestBatteryLocalCode());
        assertEquals(OrderCodingSystem.WIN_PATH.name(), order.getTestBatteryCodingSystem());
        assertTrue(order.getOrderStatus().isEmpty());
    }

    @Test
    void testOrmO01SCTimes() throws Exception {
        LabOrderMsg order = labReader.getFirstOrder(FILE_TEMPLATE, "orm_o01_sc");
        assertEquals(collectionTime, order.getCollectionDateTime());
        assertNotNull(order.getStatusChangeTime());
        assertTrue(order.getSampleReceivedTime().isSave());

        assertTrue(order.getOrderDateTime().isUnknown());
        assertTrue(order.getRequestedDateTime().isUnknown());
    }

    @Test
    void testOrmO01SCLabNumbers() throws Exception {
        LabOrderMsg order = labReader.getFirstOrder(FILE_TEMPLATE, "orm_o01_sc");
        assertEquals(InterchangeValue.buildFromHl7(epicOrder), order.getEpicCareOrderNumber());
        assertEquals(labOrder, order.getLabSpecimenNumber());
    }

    @Test
    void testOrmO01SCOrderInfo() throws Exception {
        LabOrderMsg order = labReader.getFirstOrder(FILE_TEMPLATE, "orm_o01_sc");
        assertEquals("WinPath", order.getSourceSystem());
        assertEquals("1", order.getLabDepartment());
        assertEquals(batteryCode, order.getTestBatteryLocalCode());
        assertEquals(OrderCodingSystem.WIN_PATH.name(), order.getTestBatteryCodingSystem());
        assertEquals("NW", order.getOrderStatus());
        assertEquals("I", order.getResultStatus());

        assertTrue(order.getSpecimenType().isUnknown());
    }

    @Test
    void testOrmO01SNTimes() throws Exception {
        LabOrderMsg order = labReader.getFirstOrder(FILE_TEMPLATE, "orm_o01_sn");
        assertEquals(collectionTime, order.getCollectionDateTime());
        assertNotNull(order.getStatusChangeTime());
        assertEquals(order.getStatusChangeTime(), order.getOrderDateTime().get());

        assertTrue(order.getRequestedDateTime().isUnknown());
        assertTrue(order.getSampleReceivedTime().isUnknown());
    }

    @Test
    void testOrmO01SNLabNumbers() throws Exception {
        LabOrderMsg order = labReader.getFirstOrder(FILE_TEMPLATE, "orm_o01_sn");
        assertEquals(labOrder, order.getLabSpecimenNumber());

        assertTrue(order.getEpicCareOrderNumber().isUnknown());
    }

    @Test
    void testOrmO01SNOrderInfo() throws Exception {
        LabOrderMsg order = labReader.getFirstOrder(FILE_TEMPLATE, "orm_o01_sn");
        assertEquals("WinPath", order.getSourceSystem());
        assertEquals("1", order.getLabDepartment());
        assertEquals(batteryCode, order.getTestBatteryLocalCode());
        assertEquals(OrderCodingSystem.WIN_PATH.name(), order.getTestBatteryCodingSystem());
        assertEquals("NW", order.getOrderStatus());
        assertEquals("I", order.getResultStatus());

        assertTrue(order.getSpecimenType().isUnknown());
    }

    /**
     * Cancel order request should have the epic lab number as a delete and the battery code information to be able to delete.
     * @throws Exception shouldn't happen
     */
    @Test
    void testOrmO01CaDeletes() throws Exception {
        LabOrderMsg order = labReader.getFirstOrder(FILE_TEMPLATE, "orm_o01_ca");
        InterchangeValue<String> epicOrderInterchangeValue = order.getEpicCareOrderNumber();
        assertTrue(epicOrderInterchangeValue.isDelete());
        assertEquals(epicOrder, epicOrderInterchangeValue.get());
        assertEquals(batteryCode, order.getTestBatteryLocalCode());
        assertEquals(OrderCodingSystem.WIN_PATH.name(), order.getTestBatteryCodingSystem());
    }

    /**
     * Cancel order request should have the epic lab number as a delete and the battery code information to be able to delete.
     * @throws Exception shouldn't happen
     */
    @Test
    void testOrmO01OcDeletes() throws Exception {
        LabOrderMsg order = labReader.getFirstOrder(FILE_TEMPLATE, "orm_o01_oc");
        InterchangeValue<String> epicOrderInterchangeValue = order.getEpicCareOrderNumber();
        assertTrue(epicOrderInterchangeValue.isDelete());
        assertEquals(epicOrder, epicOrderInterchangeValue.get());
        assertEquals(batteryCode, order.getTestBatteryLocalCode());
        assertEquals(OrderCodingSystem.WIN_PATH.name(), order.getTestBatteryCodingSystem());
    }

    @Test
    void testOrrO02NATimes() throws Exception {
        LabOrderMsg order = labReader.getFirstOrder(FILE_TEMPLATE, "orr_o01_na");
        assertEquals(collectionTime, order.getCollectionDateTime());
        assertNotNull(order.getStatusChangeTime());
        assertTrue(order.getRequestedDateTime().isSave());

        assertTrue(order.getOrderDateTime().isUnknown());
        assertTrue(order.getSampleReceivedTime().isUnknown());
    }

    @Test
    void testOrrO02NALabNumbers() throws Exception {
        LabOrderMsg order = labReader.getFirstOrder(FILE_TEMPLATE, "orr_o01_na");
        assertEquals(InterchangeValue.buildFromHl7(epicOrder), order.getEpicCareOrderNumber());
        assertEquals(labOrder, order.getLabSpecimenNumber());
    }

    @Test
    void testOrrO02NAOrderInfo() throws Exception {
        LabOrderMsg order = labReader.getFirstOrder(FILE_TEMPLATE, "orr_o01_na");
        assertEquals("WinPath", order.getSourceSystem());
        assertEquals(batteryCode, order.getTestBatteryLocalCode());
        assertEquals(OrderCodingSystem.WIN_PATH.name(), order.getTestBatteryCodingSystem());
        assertEquals("1039282726", order.getVisitNumber());

        assertTrue(order.getOrderStatus().isEmpty());
        assertTrue(order.getResultStatus().isEmpty());
        assertTrue(order.getSpecimenType().isUnknown());
        assertTrue(order.getLabDepartment().isEmpty());
    }

    /**
     * Cancel order request should have the epic lab number as a delete and the battery code information to be able to delete.
     * @throws Exception shouldn't happen
     */
    @Test
    void testOrrO02CRDeletes() throws Exception {
        LabOrderMsg order = labReader.getFirstOrder(FILE_TEMPLATE, "orr_o02_cr");
        InterchangeValue<String> epicOrderInterchangeValue = order.getEpicCareOrderNumber();
        assertTrue(epicOrderInterchangeValue.isDelete());
        assertEquals(epicOrder, epicOrderInterchangeValue.get());
        assertEquals(batteryCode, order.getTestBatteryLocalCode());
        assertEquals(OrderCodingSystem.WIN_PATH.name(), order.getTestBatteryCodingSystem());
    }

    /**
     * Order notes should be parsed for questions.
     * @throws Exception shouldn't happen
     */
    @Test
    void testOrmO01Notes() throws Exception {
        LabOrderMsg order = labReader.getFirstOrder(FILE_TEMPLATE, "orm_o01_questions");
        assertEquals(3, order.getQuestions().size());
        // separated by ?-
        assertEquals("Surveillance", order.getQuestions().get("Why is this test being performed"));
        // separated by :-
        assertEquals("?stroke", order.getQuestions().get("Clinical Details"));
    }

    /**
     * Null OrderControlId should throw an HL7InconsistencyException
     */
    @Test
    void testNullOrderControlId() {
        assertThrows(Hl7InconsistencyException.class, () -> labReader.getFirstOrder(FILE_TEMPLATE, "null_order_control_id"));
    }
}
