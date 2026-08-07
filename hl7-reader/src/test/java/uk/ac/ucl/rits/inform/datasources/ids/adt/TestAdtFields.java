package uk.ac.ucl.rits.inform.datasources.ids.adt;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import uk.ac.ucl.rits.inform.datasources.ids.TestHl7MessageStream;
import uk.ac.ucl.rits.inform.interchange.InterchangeValue;
import uk.ac.ucl.rits.inform.interchange.adt.AdmitPatient;
import uk.ac.ucl.rits.inform.interchange.adt.AdtMessage;
import uk.ac.ucl.rits.inform.interchange.adt.RegisterPatient;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;


/**
 * Take an HL7 ADT message as input, and check the correctness of the resultant
 * interchange message (AdtMessage).
 */
public class TestAdtFields extends TestHl7MessageStream {
    private AdtMessage msg;

    @BeforeEach
    public void setup() throws Exception {
        msg = processSingleAdtMessage("Adt/TestForJunit.txt");
    }

    /**
     * Test EVN-2.
     */
    @Test
    public void testGetRecordedDateTime1() {
        Instant result = msg.getRecordedDateTime();
        assertEquals(Instant.parse("2012-09-21T17:43:00.00Z"), result);
    }

    /**
     * Test EVN-4.
     */
    @Test
    public void testGetEventReasonCode1() {
        String result = msg.getEventReasonCode();
        assertEquals("ADM", result);
    }

    /**
     * Test EVN-6 EventOccurred - for Epic only, A02 messages.
     */
    @Test
    public void testGetEventOccurred1() {
        Instant result = msg.getEventOccurredDateTime();
        assertEquals(Instant.parse("2012-01-01T12:00:00.00Z"), result);
    }

    /**
     * Test event code
     */
    @Test
    public void testAdtClass() {
        assertTrue(msg instanceof AdmitPatient);
    }

    /**
     * PID-3.1[1] - in Carecast this is the MRN. Will Epic follow this convention?
     */
    @Test
    public void testGetPatientMrn() {
        String result = msg.getMrn();
        assertEquals("50032556", result);
    }

    /**
     * PID-3.1[2] - in Carecast this is the NHS number. Will Epic follow this convention?
     */
    @Test
    public void testGetPatientNHSNumber() {
        String result = msg.getNhsNumber();
        assertEquals("this is a test NHS number", result);
    }

    /**
     * PID-5.1.
     */
    @Test
    public void testGetPatientFamilyName1() {
        String result = msg.getPatientFamilyName().get();
        assertEquals("INTERFACES", result);
    }

    /**
     * PID-5.2.
     */
    @Test
    public void testGetPatientGivenName1() {
        String result = msg.getPatientGivenName().get();
        assertEquals("Amendadmission", result);
    }

    /**
     * PID-5.3 Middle name or initial.
     */
    @Test
    public void testGetPatientMiddleName1() {
        String result = msg.getPatientMiddleName().get();
        assertEquals("Longforenamesecondfn", result);
    }

    /**
     * PID-5.5 Title.
     */
    @Test
    public void testGetPatientTitle1() {
        String result = msg.getPatientTitle().get();
        assertEquals("LADY", result);
    }

    /**
     * PV1-19.
     */
    @Test
    public void testGetVisitNumber1() {
        String result = msg.getVisitNumber();
        assertEquals("1234TESTVISITNUM", result);
    }


    /**
     * PV1-44.1.
     */
    @Test
    public void testGetAdmissionDateTime1OrRegisterPatient() {
        if (msg instanceof AdmitPatient) {
            AdmitPatient admitPatient = (AdmitPatient) msg;
            assertEquals(
                    Instant.parse("2012-09-21T17:40:00.00Z"),
                    admitPatient.getAdmissionDateTime().get());
        }
        if (msg instanceof RegisterPatient) {
            RegisterPatient registerPatient = (RegisterPatient) msg;
            assertEquals(
                    Instant.parse("2012-09-21T17:40:00.00Z"),
                    registerPatient.getPresentationDateTime().get());
        }
    }


    /**
     * Death time should be null.
     */
    @Test
    public void testNoTimeOfDeath() {
        assertEquals(InterchangeValue.unknown(), msg.getPatientDeathDateTime());
    }

    /**
     * Death indicator should be false.
     */
    @Test
    public void testDeathIndicator() {
        assertTrue(msg.getPatientIsAlive().get());
    }

    @Test
    void testEthnicGroup() {
        assertEquals(InterchangeValue.buildFromHl7("Not Yet Asked"), msg.getEthnicGroup());
    }

    @Test
    void testAdmissionType() {
        assertEquals(InterchangeValue.buildFromHl7("Elective"), msg.getAdmissionType());
    }
}
