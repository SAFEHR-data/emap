package uk.ac.ucl.rits.inform.datasources.waveform_generator.patient_model;

import lombok.Getter;
import lombok.Setter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.ac.ucl.rits.inform.datasources.waveform.LocationMapping;
import uk.ac.ucl.rits.inform.interchange.InterchangeValue;
import uk.ac.ucl.rits.inform.interchange.adt.AdmitPatient;
import uk.ac.ucl.rits.inform.interchange.adt.AdtMessage;
import uk.ac.ucl.rits.inform.interchange.adt.DischargePatient;
import uk.ac.ucl.rits.inform.interchange.adt.TransferPatient;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Random;

public class PatientDetails {
    private final Logger logger = LoggerFactory.getLogger(getClass());
    // try to get the same numbers each time
    private final Random random;

    // some values are just opaque strings because we don't need to do any processing on them
    @Getter
    private final LocalDate dob;
    @Getter
    private final String mrn;
    @Getter
    private final String csn;
    // need to track admit time as non-admit HL7 ADT messages require it
    @Getter
    private final Instant admitDatetime;

    // datetime for latest event (transfer, discharge, etc)
    @Getter @Setter
    private Instant eventDatetime = null;

    private final LocationMapping locationMapping = new LocationMapping();

    /**
     * Current location if being discharged, new location for admit and transfer.
     */
    @Getter @Setter
    private String location = null;

    /**
     * @return location as it would be represented in our HL7 ADT feed.
     */
    public String getAdtLocation() {
        if (location == null) {
            // just use something, it doesn't really matter
            return "UNKNOWN_ADT_LOCATION";
        }
        return locationMapping.hl7AdtLocationFromCapsuleLocation(location);
    }

    /**
     * Create new synthetic patient.
     *
     * @param admitDatetime admit time to use for patient
     * @param random random object to use
     */
    public PatientDetails(Instant admitDatetime, Random random) {
        this.admitDatetime = admitDatetime;
        this.random = random;
        this.dob = LocalDate.parse("1980-01-01");
        this.mrn = makeFakeMrn();
        this.csn = makeFakeCsn();
    }

    private String makeFakeMrn() {
        // make fake data look obviously fake
        return String.format("FAKE%07d", random.nextInt(10_000_000));
    }

    private String makeFakeCsn() {
        // make fake data look obviously fake
        return String.format("FAKE%010d", random.nextLong(10_000_000_000L));
    }

    /**
     * Consumer should call to mark last event as having been processed.
     */
    public void clearLastEvent() {
        this.eventDatetime = null;
        this.location = null;
    }

    private String hl7Datetime(Instant instant) {
        LocalDateTime localDateTime = instant.atZone(ZoneId.systemDefault()).toLocalDateTime();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");
        return localDateTime.format(formatter);
    }

    private void setGenericAdtFields(AdtMessage adtMessage) {
        adtMessage.setSourceMessageId(String.format(
                "ADT_%s_%s", admitDatetime.toEpochMilli(), Instant.now().toEpochMilli()));
        adtMessage.setSourceSystem("synthetic_waveform_generator_ADT");
        adtMessage.setEventOccurredDateTime(getEventDatetime());
        adtMessage.setMrn(getMrn());
        adtMessage.setPatientBirthDate(new InterchangeValue<>(getDob()));
        adtMessage.setVisitNumber(getCsn());
        adtMessage.setRecordedDateTime(getEventDatetime());
        adtMessage.setNhsNumber("111111111");

        logger.info("Generic ADT Fields: {}", adtMessage);
    }

    /**
     * Interpret this set of details as if a transfer had just happened.
     * @return a transfer interchange message representing the transfer
     */
    public TransferPatient makeTransferMessage() {
        TransferPatient tr = new TransferPatient();
        setGenericAdtFields(tr);
        tr.setAdmissionDateTime(new InterchangeValue<>(getAdmitDatetime()));
        tr.setFullLocationString(new InterchangeValue<>(getAdtLocation()));
        return tr;
    }

    /**
     * Interpret this set of patient details as if a discharge had just happened.
     * @return a discharge interchange message representing the discharge
     */
    public DischargePatient makeDischargeMessage() {
        DischargePatient disch = new DischargePatient();
        setGenericAdtFields(disch);

        disch.setAdmissionDateTime(new InterchangeValue<>(getAdmitDatetime()));
        disch.setDischargeLocation(getLocation());
        disch.setDischargeDateTime(getEventDatetime());
        return disch;
    }

    /**
     * Interpret this set of patient details as if an admit had just happened.
     * @return an admit interchange message representing the admission
     */
    public AdmitPatient makeAdmitMessage() {
        AdmitPatient admitPatient = new AdmitPatient();
        setGenericAdtFields(admitPatient);
        admitPatient.setFullLocationString(new InterchangeValue<>(getAdtLocation()));
        admitPatient.setAdmissionDateTime(new InterchangeValue<>(getAdmitDatetime()));

        return admitPatient;
    }
}
