package uk.ac.ucl.rits.inform.datasources.ids.adt;

import org.junit.jupiter.api.Test;
import uk.ac.ucl.rits.inform.datasources.ids.TestHl7MessageStream;
import uk.ac.ucl.rits.inform.interchange.InterchangeValue;
import uk.ac.ucl.rits.inform.interchange.adt.AdmitPatient;
import uk.ac.ucl.rits.inform.interchange.adt.AdtMessage;
import uk.ac.ucl.rits.inform.interchange.adt.HospitalService;
import uk.ac.ucl.rits.inform.interchange.adt.PatientClass;
import uk.ac.ucl.rits.inform.interchange.adt.RegisterPatient;
import uk.ac.ucl.rits.inform.interchange.adt.UpdatePatientInfo;

import java.time.Instant;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;


/**
 * Test some basic things about different ADT messages. Most of the fields are already
 * tested in TestAdt so no real need to do them all again here.
 */
public class TestMiscAdt extends TestHl7MessageStream {

    private static final LocalDate BST_BIRTH_DATE = LocalDate.parse("1925-07-04");
    private static final LocalDate GMT_BIRTH_DATE = LocalDate.parse("1980-01-01");
    private static final Instant BST_BIRTH_DATETIME = Instant.parse("1925-07-04T03:44:01Z");
    private static final Instant GMT_BIRTH_DATETIME = Instant.parse("1980-01-01T03:44:01Z");


    private Instant getStartOfDay(Instant datetime) {
        return datetime.truncatedTo(ChronoUnit.DAYS);
    }

    /**
     * A04 basics.
     */
    @Test
    public void testOutpatientRegistration() throws Exception {
        AdtMessage msg = processSingleAdtMessage("Adt/generic/A04.txt");
        assertEquals(PatientClass.OUTPATIENT, msg.getPatientClass().get());
        // A04 is considered the same sort of event as A01, although the patient class
        // is usually different.
        assertEquals(RegisterPatient.class.getName(), msg.getMessageType());
    }

    /**
     * A01 basics.
     */
    @Test
    public void testInpatientAdmission() throws Exception {
        AdtMessage msg = processSingleAdtMessage("Adt/generic/A01.txt");
        assertEquals(PatientClass.INPATIENT, msg.getPatientClass().get());
        assertTrue(msg instanceof AdmitPatient);
        assertEquals(AdmitPatient.class.getName(), msg.getMessageType());
    }

    /**
     * A01 carries PV1-10, so should populate hospital service on the interchange message.
     * @throws Exception shouldn't happen
     */
    @Test
    void testAdmissionHospitalService() throws Exception {
        AdmitPatient msg = (AdmitPatient) processSingleAdtMessage("Adt/generic/A01.txt");
        assertEquals(InterchangeValue.buildFromHl7("Anaes"), msg.getHospitalService());
    }

    /**
     * A08 carries PV1-10, so should populate hospital service on the interchange message.
     * @throws Exception shouldn't happen
     */
    @Test
    void testUpdateInfoHospitalService() throws Exception {
        UpdatePatientInfo msg = (UpdatePatientInfo) processSingleAdtMessage("Adt/generic/A08_v1.txt");
        assertEquals(InterchangeValue.buildFromHl7("Anaes"), msg.getHospitalService());
    }

    /**
     * A08 can legitimately have no PV1 segment at all (a demographics-only update).
     * Hospital service should resolve to unknown rather than the message failing to parse.
     * @throws Exception shouldn't happen
     */
    @Test
    void testUpdateInfoNoPv1HospitalServiceUnknown() throws Exception {
        HospitalService msg = (HospitalService) processSingleAdtMessage("Adt/generic/A08_no_pv1.txt");
        assertTrue(msg.getHospitalService().isUnknown());
    }

    /**
     * When a birth datetime is purely a date, should keep the original day, even when BST.
     * @throws Exception shouldn't happen
     */
    @Test
    void testBSTBirthDate() throws Exception {
        AdtMessage msg = processSingleAdtMessage("Adt/birth_date_bst.txt");
        assertEquals(InterchangeValue.buildFromHl7(BST_BIRTH_DATE), msg.getPatientBirthDate());
        assertEquals(InterchangeValue.buildFromHl7(getStartOfDay(BST_BIRTH_DATETIME)), msg.getPatientBirthDateTime());

    }

    /**
     * When a birth datetime is purely a explicitly midnight on a day, the date should remain the same and the instant should be converted.
     * @throws Exception shouldn't happen
     */
    @Test
    void testBSTBirthExplicitMidnight() throws Exception {
        AdtMessage msg = processSingleAdtMessage("Adt/birth_datetime_midnight_bst.txt");
        assertEquals(InterchangeValue.buildFromHl7(BST_BIRTH_DATE), msg.getPatientBirthDate());
        assertEquals(InterchangeValue.buildFromHl7(getStartOfDay(BST_BIRTH_DATETIME).minus(1, ChronoUnit.HOURS)), msg.getPatientBirthDateTime());
    }


    /**
     * When a birth datetime has time information, should be converted to UTC.
     * @throws Exception shouldn't happen
     */
    @Test
    void testBSTBirthDateTime() throws Exception {
        AdtMessage msg = processSingleAdtMessage("Adt/birth_datetime_bst.txt");
        assertEquals(InterchangeValue.buildFromHl7(BST_BIRTH_DATE), msg.getPatientBirthDate());
        assertEquals(InterchangeValue.buildFromHl7(BST_BIRTH_DATETIME), msg.getPatientBirthDateTime());
    }

    /**
     * When a birth datetime is purely a date, should keep original day.
     * @throws Exception shouldn't happen
     */
    @Test
    void testGMTBirthDate() throws Exception {
        AdtMessage msg = processSingleAdtMessage("Adt/birth_date_gmt.txt");
        assertEquals(InterchangeValue.buildFromHl7(GMT_BIRTH_DATE), msg.getPatientBirthDate());
        assertEquals(InterchangeValue.buildFromHl7(getStartOfDay(GMT_BIRTH_DATETIME)), msg.getPatientBirthDateTime());

    }

    /**
     * When a birth datetime has time information in GMT, should keep original datetime as already in UTC.
     * @throws Exception shouldn't happen
     */
    @Test
    void testGMTBirthDateTime() throws Exception {
        AdtMessage msg = processSingleAdtMessage("Adt/birth_datetime_gmt.txt");
        assertEquals(InterchangeValue.buildFromHl7(GMT_BIRTH_DATE), msg.getPatientBirthDate());
        assertEquals(InterchangeValue.buildFromHl7(GMT_BIRTH_DATETIME), msg.getPatientBirthDateTime());
    }

}
