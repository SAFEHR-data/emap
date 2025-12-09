package uk.ac.ucl.rits.inform.datasources.ids.hl7.parser;

import ca.uhn.hl7v2.HL7Exception;
import ca.uhn.hl7v2.model.v26.datatype.PL;
import ca.uhn.hl7v2.model.v26.segment.PV1;
import uk.ac.ucl.rits.inform.datasources.ids.Doctor;
import uk.ac.ucl.rits.inform.datasources.ids.HL7Utils;

import java.time.Instant;
import java.util.Vector;

/**
 * Wrapper around the HAPI parser's PV1 segment object, to make it easier to use.
 * Reference page: https://hapifhir.github.io/hapi-hl7v2/v27/apidocs/ca/uhn/hl7v2/model/v27/segment/PV1.html
 */
public interface PV1Wrap {
    /**
     * @return the PV1 object
     */
    PV1 getPV1();

    /**
     * @return whether the PV1 segment exists
     */
    default boolean pv1SegmentExists() {
        return getPV1() != null;
    }

    /**
     * @return PV1-2 patient class
     * @throws HL7Exception if HAPI does
     */
    default String getPatientClass() throws HL7Exception {
        if (!pv1SegmentExists()) {
            return null;
        }
        return getPV1().getPatientClass().getValue();
    }

    /**
     * @param location HAPI location object
     * @return Ward Code from patient location e.g. T06
     */
    default String getWardCode(PL location) {
        return location.getPl1_PointOfCare().getValue();
    }

    /**
     * @param location HAPI location object
     * @return Room Code e.g. T06A
     */
    default String getRoomCode(PL location) {
        return location.getPl2_Room().getValue();
    }

    /**
     * @param location HAPI location object
     * @return Bed e.g. T06-32
     */
    default String getBed(PL location) {
        return location.getPl3_Bed().getValue();
    }

    private String createLocationString(PL currentLocation) {
        String joinedLocations = String.join("^", getWardCode(currentLocation), getRoomCode(currentLocation), getBed(currentLocation));
        return ("null^null^null".equals(joinedLocations)) ? null : joinedLocations;
    }

    /**
     * PV1-3: Get all the location components concatenated together.
     * @return the location string
     */
    default String getFullLocationString() {
        if (!pv1SegmentExists()) {
            return null;
        }
        PL currentLocation = getPV1().getAssignedPatientLocation();
        return createLocationString(currentLocation);
    }

    /**
     * PV1-4: Admission type.
     * @return the admission type string
     */
    default String getAdmissionType() {
        if (!pv1SegmentExists()) {
            return null;
        }
        return getPV1().getAdmissionType().getValue();
    }

    /**
     * PV1-6: Previous patient location concatenated together.
     * @return the previous location string
     */
    default String getPreviousLocation() {
        PL previousLocation = getPV1().getPriorPatientLocation();
        return createLocationString(previousLocation);
    }

    /**
     * Get the attending doctor(s) PV1-7.1 to PV1-7.7.
     * @return Vector of Doctor objects
     * @throws HL7Exception if HAPI does
     */
    default Vector<Doctor> getAttendingDoctors() throws HL7Exception {
        Vector<Doctor> v = new Vector<Doctor>();
        int reps = getPV1().getAttendingDoctorReps();
        for (int i = 0; i < reps; i++) {
            Doctor dr = new Doctor(getPV1().getAttendingDoctor(i));
            v.add(dr);
        }
        return v;
    }

    /**
     * Get Referring Doctor(s) PV1-8.1 to PV1-8.6
     * <p>
     * I am not sure if Carecast uses PV1-8.7.
     * @return Vector of Doctor objects
     * @throws HL7Exception if HAPI does
     */
    default Vector<Doctor> getReferringDoctors() throws HL7Exception {
        int reps = getPV1().getReferringDoctorReps();
        Vector<Doctor> v = new Vector<Doctor>(reps);
        for (int i = 0; i < reps; i++) {
            Doctor dr = new Doctor(getPV1().getReferringDoctor(i));
            v.add(dr);
        }
        return v;
    }

    /**
     * @return PV1-10 Hospital Service Specialty eg. 31015
     * @throws HL7Exception if HAPI does
     */
    default String getHospitalService() throws HL7Exception {
        return getPV1().getHospitalService().getValue();
    }

    /**
     * @return PV1-14 Admission Source. NB Carecast says ZLC8.1 Source of admission
     * @throws HL7Exception if HAPI does
     */
    default String getAdmitSource() throws HL7Exception {
        return getPV1().getAdmitSource().getValue();
    }

    /**
     * @return PV1-18 Patient Type
     * @throws HL7Exception if HAPI does
     */
    default String getPatientType() throws HL7Exception {
        return getPV1().getPatientType().getValue();
    }

    /**
     * @return PV1-19 Visit number
     * @throws HL7Exception if HAPI does
     */
    default String getVisitNumber() throws HL7Exception {
        return getPV1().getVisitNumber().getCx1_IDNumber().getValueOrEmpty();
    }

    /**
     * @return discharge disposition PV1-36
     */
    default String getDischargeDisposition() {
        return getPV1().getPv136_DischargeDisposition().getValueOrEmpty();
    }

    /**
     * @return discharge location identifier PV1-37
     */
    default String getDischargeLocation() {
        return getPV1().getPv137_DischargedToLocation().getDld1_DischargeToLocation().getCwe1_Identifier().getValueOrEmpty();
    }

    /**
     * @return concatenated pending location string
     */
    default String getPendingLocation() {
        PL pendingLocation = getPV1().getPv142_PendingLocation();
        return createLocationString(pendingLocation);
    }

    /**
     * We could probably use the HAPI functions to obtain the different components of this result,
     * e.g. for easier conversion to Postgres timestamp format.
     * @return PV1-44.1 admission datetime
     * @throws HL7Exception if HAPI does
     */
    default Instant getAdmissionDateTime() throws HL7Exception {
        return HL7Utils.interpretLocalTime(getPV1().getAdmitDateTime());
    }

    /**
     * @return PV1-45.1 discharge datetime
     * @throws HL7Exception if HAPI does
     */
    default Instant getDischargeDateTime() throws HL7Exception {
        return HL7Utils.interpretLocalTime(getPV1().getDischargeDateTime());
    }
}
