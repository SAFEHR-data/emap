package uk.ac.ucl.rits.inform.datasinks.emapstar;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.springframework.transaction.annotation.Transactional;

import uk.ac.ucl.rits.inform.datasinks.emapstar.exceptions.MessageIgnoredException;
import uk.ac.ucl.rits.inform.datasinks.emapstar.repos.CoreDemographicRepository;
import uk.ac.ucl.rits.inform.datasinks.emapstar.repos.HospitalVisitRepository;
import uk.ac.ucl.rits.inform.datasinks.emapstar.repos.LocationVisitRepository;
import uk.ac.ucl.rits.inform.datasinks.emapstar.repos.MrnRepository;
import uk.ac.ucl.rits.inform.interchange.EmapOperationMessage;
import uk.ac.ucl.rits.inform.interchange.EmapOperationMessageProcessingException;
import uk.ac.ucl.rits.inform.interchange.visit_observations.Flowsheet;
import uk.ac.ucl.rits.inform.interchange.InterchangeValue;
import uk.ac.ucl.rits.inform.interchange.ValueType;
import uk.ac.ucl.rits.inform.interchange.adt.AdmitPatient;
import uk.ac.ucl.rits.inform.interchange.adt.CancelAdmitPatient;
import uk.ac.ucl.rits.inform.interchange.adt.CancelDischargePatient;
import uk.ac.ucl.rits.inform.interchange.adt.CancelTransferPatient;
import uk.ac.ucl.rits.inform.interchange.adt.DischargePatient;
import uk.ac.ucl.rits.inform.interchange.adt.MergePatient;
import uk.ac.ucl.rits.inform.interchange.adt.PatientClass;
import uk.ac.ucl.rits.inform.interchange.adt.RegisterPatient;
import uk.ac.ucl.rits.inform.interchange.adt.TransferPatient;
import uk.ac.ucl.rits.inform.interchange.adt.UpdatePatientInfo;

/**
 * Create a stream of messages to simulate a chain of HL7 messages firing.
 *
 * @author Jeremy Stein & Roma Klapaukh
 */
@SpringJUnitConfig
@SpringBootTest
@ActiveProfiles("test")
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
public abstract class MessageStreamBaseCase {

    @Autowired
    protected InformDbOperations         dbOps;

    @Autowired
    protected MrnRepository              mrnRepository;

    @Autowired
    protected HospitalVisitRepository    hospitalVisitRepository;

    @Autowired
    protected CoreDemographicRepository  coreDemographicRepository;

    @Autowired
    protected LocationVisitRepository    locationVisitRepository;

    protected List<EmapOperationMessage> messageStream                = new ArrayList<>();
    /**
     * How far though the message stream processing is.
     */
    protected int                        nextToProcess                = 0;

    protected Instant                    currentTime                  = Instant.parse("2020-03-01T06:30:00.000Z");
    protected final String[]             allLocations                 = {"T42^BADGERS^WISCONSIN", "ED^BADGERS^HONEY",
            "ED^BADGERS^HOG", "ED^BADGERS^PALAWAN", "ED^BADGERS^JAPANESE", "ED^BADGERS^JAVAN", "ED^BADGERS^EURASIAN" };
    protected int                        currentLocation              = 0;
    protected String                     mrn                          = "1234ABCD";
    protected String                     csn                          = "1234567890";
    private InterchangeValue<PatientClass> patientClass                 = new InterchangeValue<>(PatientClass.EMERGENCY);
    private Instant                      latestPatientClassChangeTime = null;
    protected InterchangeValue<Instant> admissionTime                = InterchangeValue.unknown();
    protected InterchangeValue<Instant> presentationTime             = InterchangeValue.unknown();
    protected Instant                    dischargeTime                = null;
    protected String                     nhsNumber                    = "9999999999";
    protected InterchangeValue<String> fName                         = new InterchangeValue<>("Fred");
    protected InterchangeValue<String> mName                         = InterchangeValue.unknown();
    protected InterchangeValue<String> lName                         = new InterchangeValue<>("Blogger");
    protected final List<Instant>        transferTime                 = new ArrayList<>();

    protected String                     dischargeDisposition         = "Peachy";
    protected String                     dischargeLocation            = "Home";
    protected InterchangeValue<Boolean> patientAlive                 = new InterchangeValue<Boolean>(true);
    protected InterchangeValue<Instant> deathTime                    = InterchangeValue.unknown();

    protected double                     vitalReading                 = 92.;
    protected List<Instant>              vitalTime                    = new ArrayList<>();

    /**
     * Create a new MessageStreamBaseCase.
     */
    public MessageStreamBaseCase() {}

    /**
     * Reset the state to allow for a new stream of stream of tests to be run with
     * the same instance.
     * <p>
     * This does not reset the class to how it is instantiated though.
     */
    protected void reinitialise() {
        messageStream.clear();
        nextToProcess = 0;
        this.patientClass = new InterchangeValue<>(PatientClass.EMERGENCY);
        latestPatientClassChangeTime = null;
        this.vitalTime.clear();
        this.transferTime.clear();
        this.admissionTime = InterchangeValue.unknown();
        this.presentationTime = InterchangeValue.unknown();
        this.dischargeTime = null;
        this.patientAlive = new InterchangeValue<>(true);
        this.deathTime = InterchangeValue.unknown();
        dischargeDisposition = "Peachy";
        dischargeLocation = "Home";
    }

    /**
     * Add a message to the queue.
     *
     * @param msg The message to add.
     */
    public void queueMessage(EmapOperationMessage msg) {
        this.messageStream.add(msg);
    }

    /**
     * Process all remaining messages in queue.
     *
     * @throws EmapOperationMessageProcessingException when message cannot be processed
     */
    @Transactional
    public void processRest() throws EmapOperationMessageProcessingException {
        for (; nextToProcess < messageStream.size(); nextToProcess++) {
            processSingleMessage(messageStream.get(nextToProcess));
        }
    }

    /**
     * Process the next n messages in the list.
     *
     * @param n Number of messages to process.
     * @throws EmapOperationMessageProcessingException  If message can't be processed
     * @throws IndexOutOfBoundsException                If n is larger than the
     *                                                  remaining number of
     *                                                  messages.
     */
    @Transactional
    public void processN(int n) throws EmapOperationMessageProcessingException {
        int end = nextToProcess + n;
        while (nextToProcess < end) {
            processSingleMessage(messageStream.get(nextToProcess++));
        }
    }

    /**
     * Process a single message.
     *
     * @param msg The message to process.
     * @throws EmapOperationMessageProcessingException when single message cannot be processed
     */
    @Transactional
    protected void processSingleMessage(EmapOperationMessage msg) throws EmapOperationMessageProcessingException {
        processSingleMessage(false, msg);
    }

    @Transactional
    protected void processSingleMessage(boolean allowMessageIgnored, EmapOperationMessage msg)
            throws EmapOperationMessageProcessingException {
        try {
            msg.processMessage(dbOps);
        } catch (MessageIgnoredException me) {
            if (!allowMessageIgnored) {
                throw me;
            }
        }
    }

    /**
     * Step the clock forward not quite exactly one hour. One hour looks too much
     * like a time zone bug when it causes an error.
     */
    protected void stepClock() {
        this.currentTime = this.currentTime.plusSeconds(3660);
    }

    /**
     * Return a time and advance the clock.
     *
     * @return A time later than all the previous ones.
     */
    protected Instant nextTime() {
        this.stepClock();
        return currentTime;
    }

    /**
     * Get the current location of the patient.
     *
     * @return Current location.
     */
    protected InterchangeValue<String> currentLocation() {
        return new InterchangeValue<>(this.allLocations[this.currentLocation]);
    }

    /**
     * Select the next location. When the end of the array is reached, loop around.
     */
    protected void stepLocation() {
        this.currentLocation = (this.currentLocation + 1) % this.allLocations.length;
    }

    /**
     * Select the previous location. When the start of the array is reached, loop
     * around.
     */
    protected void backLocation() {
        this.currentLocation = this.currentLocation - 1;
        if (this.currentLocation < 0) {
            this.currentLocation = this.allLocations.length + this.currentLocation;
        }
    }

    /**
     * Get the current location and advance to the next one.
     *
     * @return A location.
     */
    protected String nextLocation() {
        this.stepLocation();
        return allLocations[currentLocation];
    }

    /**
     * See the next location without changing state. When the end of the array is
     * reached, loop around.
     *
     * @return The next location coming up in the list
     */
    protected String peekNextLocation() {
        int loc = (this.currentLocation + 1) % this.allLocations.length;
        return allLocations[loc];
    }

    /**
     * Return to the previous location and return it.
     *
     * @return A location.
     */
    protected InterchangeValue<String> previousLocation() {
        this.backLocation();
        return new InterchangeValue<>(allLocations[currentLocation]);
    }

    /**
     * Get the time of the last transfer.
     *
     * @return The time of the last transfer
     */
    protected Instant lastTransferTime() {
        if (this.transferTime.isEmpty()) {
            return null;
        }
        return this.transferTime.get(this.transferTime.size() - 1);
    }

    /**
     * Make sure that the patient has an admission time.
     */
    protected void ensureAdmitted() {
        if (this.admissionTime.isUnknown()) {
            this.admissionTime = new InterchangeValue<>(this.nextTime());
        }
    }

    /**
     * Queue a vital signs message.
     */
    public void queueFlowsheet() {
        Instant vitalTime = this.nextTime();
        this.vitalTime.add(vitalTime);

        Flowsheet vital = new Flowsheet();
        vital.setFlowsheetId("448867");
        vital.setSourceSystem("EPIC");
        vital.setMrn(this.mrn);
        vital.setVisitNumber(this.csn);
        vital.setSourceObservationType("flowsheet");
        vital.setNumericValue(InterchangeValue.buildFromHl7(vitalReading));
        vital.setUnit(InterchangeValue.buildFromHl7("/min"));
        vital.setObservationTime(vitalTime);
        vital.setUpdatedTime(vitalTime);
        vital.setValueType(ValueType.NUMERIC);
        queueMessage(vital);
    }

    protected void queueUpdatePatientDetails() {
        this.queueUpdatePatientDetails(getPatientClass());
    }

    /**
     * Queue a patient update message.
     *
     * @param patientClass the patient class to set
     */
    public void queueUpdatePatientDetails(InterchangeValue<PatientClass> patientClass) {
        boolean impliedTransfer = this.transferTime.isEmpty();

        // clock must be changed before anything which might cause a change
        this.stepClock();

        if (impliedTransfer) {
            this.transferTime.add(this.currentTime);
        }
        setPatientClass(patientClass, this.currentTime);

        UpdatePatientInfo update = new UpdatePatientInfo();
        update.setRecordedDateTime(this.currentTime);
        update.setEventOccurredDateTime(this.currentTime);
        update.setSourceSystem("EPIC");
        update.setMrn(this.mrn);
        update.setNhsNumber(this.nhsNumber);
        update.setVisitNumber(this.csn);
        update.setPatientGivenName(this.fName);
        update.setPatientMiddleName(this.mName);
        update.setPatientFamilyName(this.lName);
        update.setFullLocationString(new InterchangeValue<>(allLocations[this.currentLocation]));
        update.setPatientClass(this.patientClass);
        update.setPatientIsAlive(this.patientAlive);
        update.setPatientDeathDateTime(this.deathTime);
        queueMessage(update);
    }

    /**
     * Queue an admit message that doesn't perform a transfer. If you want to
     * simulate an A06 change do <code>patientClass="I"</code> before calling this
     * method.
     */
    public void queueAdmit() {
        this.queueAdmit(false, getPatientClass());
    }

    /**
     * Queue an admit message that may or may not perform a transfer.
     *
     * @param transfer If true will also advance the patient to the next location.
     */
    public void queueAdmit(boolean transfer) {
        this.queueAdmit(transfer, getPatientClass());
    }

    /**
     * Queue an admit message that may or may not perform a transfer.
     *
     * @param transfer     If true will also advance the patient to the next
     *                     location.
     * @param patientClass the patient class for this admit.
     */
    public void queueAdmit(boolean transfer, InterchangeValue<PatientClass> patientClass) {
        Instant eventTime = this.nextTime();
        setPatientClass(patientClass, eventTime);

        if (this.transferTime.isEmpty() || transfer) {
            this.transferTime.add(eventTime);
        }
        if (this.admissionTime.isUnknown()) {
            this.admissionTime = new InterchangeValue<Instant>(eventTime);
        }
        if (transfer) {
            this.stepLocation();
        }

        AdmitPatient admit = new AdmitPatient();
        admit.setAdmissionDateTime(this.admissionTime);
        admit.setEventOccurredDateTime(eventTime);
        admit.setRecordedDateTime(eventTime);
        admit.setSourceSystem("EPIC");
        admit.setMrn(this.mrn);
        admit.setVisitNumber(this.csn);
        admit.setPatientClass(this.getPatientClass());
        admit.setPatientGivenName(this.fName);
        admit.setPatientMiddleName(this.mName);
        admit.setPatientFamilyName(this.lName);
        admit.setFullLocationString(this.currentLocation());
        admit.setPatientIsAlive(this.patientAlive);
        admit.setPatientDeathDateTime(this.deathTime);
        this.queueMessage(admit);
    }

    /**
     * Queue a registration message that does not perform a transfer.
     *
     * @param patientClass the patient class for this registration.
     */
    public void queueRegister(InterchangeValue<PatientClass> patientClass) {
        Instant eventTime = this.nextTime();
        setPatientClass(patientClass, eventTime);

        if (this.transferTime.isEmpty()) {
            this.transferTime.add(eventTime);
        }
        if (this.presentationTime.isUnknown()) {
            this.presentationTime = new InterchangeValue<Instant>(eventTime);
        }

        RegisterPatient register = new RegisterPatient();

        register.setPresentationDateTime(this.presentationTime);
        register.setEventOccurredDateTime(eventTime);
        register.setRecordedDateTime(eventTime);
        register.setSourceSystem("EPIC");
        register.setMrn(this.mrn);
        register.setVisitNumber(this.csn);
        register.setPatientClass(this.getPatientClass());
        register.setPatientGivenName(this.fName);
        register.setPatientMiddleName(this.mName);
        register.setPatientFamilyName(this.lName);
        register.setFullLocationString(this.currentLocation());
        register.setPatientIsAlive(this.patientAlive);
        register.setPatientDeathDateTime(this.deathTime);
        this.queueMessage(register);
    }

    /**
     * Queue a moving transfer message.
     */
    public void queueTransfer() {
        this.queueTransfer(true, getPatientClass());
    }

    /**
     * Queue a transfer message.
     *
     * @param updateLocation if false the patient location will not be advanced.
     */
    public void queueTransfer(boolean updateLocation) {
        this.queueTransfer(updateLocation, getPatientClass());
    }

    /**
     * Queue a transfer message.
     *
     * @param updateLocation if false the patient location will not be advanced.
     * @param patientClass   The patient class for the transfered patient
     */
    public void queueTransfer(boolean updateLocation, InterchangeValue<PatientClass> patientClass) {
        this.ensureAdmitted();

        // Handle non-moving update
        InterchangeValue<String> location;

        Instant tTime = this.nextTime();
        setPatientClass(patientClass, tTime);

        if (updateLocation) {
            location = new InterchangeValue<>(nextLocation());
            this.transferTime.add(tTime);
        } else {
            location = currentLocation();
        }

        TransferPatient transfer = new TransferPatient();
        transfer.setAdmissionDateTime(this.admissionTime);
        transfer.setEventOccurredDateTime(tTime);
        transfer.setRecordedDateTime(tTime);
        transfer.setSourceSystem("EPIC");
        transfer.setMrn(this.mrn);
        transfer.setVisitNumber(this.csn);
        transfer.setPatientClass(this.getPatientClass());
        transfer.setPatientGivenName(this.fName);
        transfer.setPatientMiddleName(this.mName);
        transfer.setPatientFamilyName(this.lName);
        transfer.setFullLocationString(location);
        transfer.setPatientIsAlive(this.patientAlive);
        transfer.setPatientDeathDateTime(deathTime);
        this.queueMessage(transfer);

    }

    /**
     * Queue a cancel admit message.
     */
    public void queueCancelAdmit() {
        this.ensureAdmitted();
        CancelAdmitPatient cancelAdmit = new CancelAdmitPatient();
        Instant expectedCancellationDateTime = this.nextTime();

        if (!this.transferTime.isEmpty()) {
            this.transferTime.remove(this.transferTime.size() - 1);
        }

        cancelAdmit.setEventOccurredDateTime(expectedCancellationDateTime);
        cancelAdmit.setRecordedDateTime(expectedCancellationDateTime);
        cancelAdmit.setSourceSystem("EPIC");
        cancelAdmit.setMrn(this.mrn);
        cancelAdmit.setVisitNumber(this.csn);
        cancelAdmit.setPatientClass(this.getPatientClass());
        cancelAdmit.setPatientGivenName(this.fName);
        cancelAdmit.setPatientMiddleName(this.mName);
        cancelAdmit.setPatientFamilyName(this.lName);
        cancelAdmit.setFullLocationString(this.previousLocation());
        cancelAdmit.setPatientIsAlive(this.patientAlive);
        cancelAdmit.setPatientDeathDateTime(deathTime);

        this.queueMessage(cancelAdmit);

        this.admissionTime = null;
    }

    /**
     * Queue a cancel transfer message.
     */
    public void queueCancelTransfer() {
        Instant erroneousTransferDateTime;
        if (!this.transferTime.isEmpty()){
            erroneousTransferDateTime = this.transferTime.remove(this.transferTime.size() - 1);
        } else {
            erroneousTransferDateTime = this.nextTime();
        }

        Instant eventTime = this.nextTime();
        CancelTransferPatient cancelTransfer = new CancelTransferPatient();
        cancelTransfer.setCancelledDateTime(eventTime);
        cancelTransfer.setEventOccurredDateTime(erroneousTransferDateTime);
        cancelTransfer.setRecordedDateTime(eventTime);
        cancelTransfer.setSourceSystem("EPIC");
        cancelTransfer.setMrn(this.mrn);
        cancelTransfer.setVisitNumber(this.csn);
        cancelTransfer.setPatientClass(this.getPatientClass());
        cancelTransfer.setPatientGivenName(this.fName);
        cancelTransfer.setPatientMiddleName(this.mName);
        cancelTransfer.setPatientFamilyName(this.lName);
        cancelTransfer.setCancelledLocation(this.currentLocation().get());
        cancelTransfer.setFullLocationString(this.previousLocation());
        cancelTransfer.setPatientIsAlive(this.patientAlive);
        cancelTransfer.setPatientDeathDateTime(deathTime);

        this.queueMessage(cancelTransfer);
    }

    /**
     * Queue a discharge message.
     */
    public void queueDischarge() {
        this.ensureAdmitted();
        this.dischargeTime = this.nextTime();

        DischargePatient discharge = new DischargePatient();

        discharge.setAdmissionDateTime(this.admissionTime);
        discharge.setEventOccurredDateTime(this.dischargeTime);
        discharge.setRecordedDateTime(this.dischargeTime);
        discharge.setSourceSystem("EPIC");
        discharge.setMrn(this.mrn);
        discharge.setFullLocationString(currentLocation());
        discharge.setPreviousLocationString(previousLocation());
        discharge.setVisitNumber(this.csn);
        discharge.setPatientClass(this.patientClass);
        discharge.setPatientGivenName(this.fName);
        discharge.setPatientMiddleName(this.mName);
        discharge.setPatientFamilyName(this.lName);
        discharge.setDischargeDisposition(this.dischargeDisposition);
        discharge.setDischargeLocation(this.dischargeLocation);
        discharge.setDischargeDateTime(this.dischargeTime);
        discharge.setPatientIsAlive(this.patientAlive);
        discharge.setPatientDeathDateTime(deathTime);

        this.queueMessage(discharge);
    }

    /**
     * Queue a cancel discharge message.
     */
    public void queueCancelDischarge() {
        Instant eventTime = this.nextTime();
        this.ensureAdmitted();
        CancelDischargePatient cancelDischarge = new CancelDischargePatient();
        cancelDischarge.setEventOccurredDateTime(eventTime);
        cancelDischarge.setRecordedDateTime(eventTime);
        cancelDischarge.setSourceSystem("EPIC");
        cancelDischarge.setMrn(this.mrn);
        cancelDischarge.setVisitNumber(this.csn);
        cancelDischarge.setPatientClass(this.getPatientClass());
        cancelDischarge.setPatientGivenName(this.fName);
        cancelDischarge.setPatientMiddleName(this.mName);
        cancelDischarge.setPatientFamilyName(this.lName);
        cancelDischarge.setFullLocationString(currentLocation());
        cancelDischarge.setPatientIsAlive(this.patientAlive);
        cancelDischarge.setPatientDeathDateTime(deathTime);
        // A13 messages do not carry the discharge time field

        this.queueMessage(cancelDischarge);
    }

    /**
     * Queue a merge MRN message.
     *
     * @param mergedMrn    The mrn that will stop being used
     * @param survivingMrn The mrn that will be used going forwards
     */
    public void queueMerge(String mergedMrn, String survivingMrn) {
        Instant eventTime = this.nextTime();

        MergePatient merge = new MergePatient();

        merge.setSourceSystem("EPIC");
        merge.setEventOccurredDateTime(eventTime);
        merge.setRecordedDateTime(eventTime);
        merge.setMrn(survivingMrn);
        merge.setPreviousMrn(mergedMrn);

        this.queueMessage(merge);
    }

    /**
     * @return the patientClass
     */
    public InterchangeValue<PatientClass> getPatientClass() {
        return patientClass;
    }

    /**
     * Smarter setter that checks for changes and updates the change time. If you
     * really want to call this from a test you have to know what time it changed.
     *
     * @param patientClass the patientClass to set
     * @param eventTime    the current event time
     */
    protected void setPatientClass(InterchangeValue<PatientClass> patientClass, Instant eventTime) {
        // Ignore not real changes.
        if (patientClass.equals(this.patientClass)) {
            return;
        }
        latestPatientClassChangeTime = eventTime;
        this.patientClass = patientClass;
    }

    /**
     * @return the latestPatientClassChangeTimeSure
     */
    public Instant getLatestPatientClassChangeTime() {
        return latestPatientClassChangeTime;
    }
}
