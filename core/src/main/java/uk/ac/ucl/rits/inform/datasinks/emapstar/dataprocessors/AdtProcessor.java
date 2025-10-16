package uk.ac.ucl.rits.inform.datasinks.emapstar.dataprocessors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import uk.ac.ucl.rits.inform.datasinks.emapstar.controllers.PatientLocationController;
import uk.ac.ucl.rits.inform.datasinks.emapstar.controllers.PendingAdtController;
import uk.ac.ucl.rits.inform.datasinks.emapstar.controllers.PersonController;
import uk.ac.ucl.rits.inform.datasinks.emapstar.controllers.VisitController;
import uk.ac.ucl.rits.inform.datasinks.emapstar.controllers.DeletionController;
import uk.ac.ucl.rits.inform.datasinks.emapstar.exceptions.RequiredDataMissingException;
import uk.ac.ucl.rits.inform.informdb.identity.HospitalVisit;
import uk.ac.ucl.rits.inform.informdb.identity.Mrn;
import uk.ac.ucl.rits.inform.interchange.EmapOperationMessageProcessingException;
import uk.ac.ucl.rits.inform.interchange.adt.AdtMessage;
import uk.ac.ucl.rits.inform.interchange.adt.CancelPendingTransfer;
import uk.ac.ucl.rits.inform.interchange.adt.ChangePatientIdentifiers;
import uk.ac.ucl.rits.inform.interchange.adt.DeletePersonInformation;
import uk.ac.ucl.rits.inform.interchange.adt.MergePatient;
import uk.ac.ucl.rits.inform.interchange.adt.MoveVisitInformation;
import uk.ac.ucl.rits.inform.interchange.adt.PendingTransfer;
import uk.ac.ucl.rits.inform.interchange.adt.SwapLocations;
import uk.ac.ucl.rits.inform.interchange.adt.UpdateSubSpeciality;

import java.time.Instant;
import java.util.List;

/**
 * Handle processing of ADT messages.
 * @author Stef Piatek
 */
@Component
public class AdtProcessor {
    private static final Logger logger = LoggerFactory.getLogger(AdtProcessor.class);
    private final PersonController personController;
    private final VisitController visitController;
    private final PatientLocationController patientLocationController;
    private final PendingAdtController pendingAdtController;
    private final DeletionController deletionController;

    /**
     * Implicitly wired spring beans.
     * @param personController              person interactions.
     * @param visitController               encounter interactions.
     * @param patientLocationController     location interactions.
     * @param pendingAdtController          pending ADT interactions.
     * @param deletionController            cascading deletes for hospital visits.
     */
    public AdtProcessor(PersonController personController, VisitController visitController,
                        PatientLocationController patientLocationController, PendingAdtController pendingAdtController,
                        DeletionController deletionController) {
        this.personController = personController;
        this.visitController = visitController;
        this.patientLocationController = patientLocationController;
        this.pendingAdtController = pendingAdtController;
        this.deletionController = deletionController;
    }


    /**
     * Default processing of an ADT message.
     * @param msg        ADT message
     * @param storedFrom time that emap-core started processing the message.
     * @throws EmapOperationMessageProcessingException if message can't be processed.
     */
    @Transactional
    public void processMessage(final AdtMessage msg, final Instant storedFrom) throws EmapOperationMessageProcessingException {
        Instant messageDateTime = msg.bestGuessAtValidFrom();
        HospitalVisit visit = processPersonAndVisit(msg, storedFrom, messageDateTime);
        patientLocationController.processVisitLocation(visit, msg, storedFrom);
    }

    private HospitalVisit processPersonAndVisit(AdtMessage msg, Instant storedFrom, Instant validFrom) throws RequiredDataMissingException {
        Mrn mrn = processPersonLevel(msg, storedFrom, validFrom);
        return visitController.updateOrCreateHospitalVisit(msg, storedFrom, mrn);
    }

    /**
     * Process person level information, saving changes to database.
     * @param msg             adt message
     * @param storedFrom      time that emap-core started processing the message.
     * @param messageDateTime date time of the message
     * @return MRN
     * @throws RequiredDataMissingException If MRN and NHS number are both null
     */
    @Transactional
    public Mrn processPersonLevel(AdtMessage msg, Instant storedFrom, Instant messageDateTime) throws RequiredDataMissingException {
        Mrn mrn = personController.getOrCreateMrn(msg.getMrn(), msg.getNhsNumber(), msg.getSourceSystem(), msg.getRecordedDateTime(), storedFrom);
        personController.updateOrCreateDemographic(mrn, msg, messageDateTime, storedFrom);
        return mrn;
    }

    /**
     * Process MergePatient message, saving changed to the database.
     * @param msg        adt message
     * @param storedFrom time that emap-core started processing the message.
     * @throws RequiredDataMissingException if the suriving MRN is null or the previous MRN's mrn and nhs number are both null.
     */
    @Transactional
    public void processMergePatient(MergePatient msg, Instant storedFrom) throws RequiredDataMissingException {
        Mrn survivingMrn = personController.getOrCreateOnMrnOnly(
                msg.getMrn(), msg.getNhsNumber(), msg.getSourceSystem(), msg.getRecordedDateTime(), storedFrom);
        personController.updateOrCreateDemographic(survivingMrn, msg, msg.bestGuessAtValidFrom(), storedFrom);
        personController.mergeMrns(msg, survivingMrn, storedFrom);

    }

    /**
     * Delete all information for a person that is older than the message.
     * <p>
     * This is being processed <a href="https://www.hl7.org/documentcenter/public/wg/conf/Msgadt.pdf">as per page 137 </a>.
     * Keeping the MRN as this may be used by another person.
     * @param msg        DeletePersonInformation
     * @param storedFrom time that emap-core started processing the message.
     * @throws RequiredDataMissingException If MRN and NHS number are both null
     */
    @Transactional
    public void deletePersonInformation(DeletePersonInformation msg, Instant storedFrom) throws RequiredDataMissingException {
        Instant messageDateTime = msg.bestGuessAtValidFrom();
        Mrn mrn = personController.getOrCreateMrn(msg.getMrn(), msg.getNhsNumber(), msg.getSourceSystem(), messageDateTime, storedFrom);
        personController.deleteDemographic(mrn, messageDateTime, storedFrom);
        deletionController.deleteMrnDependentEntities(mrn, messageDateTime, storedFrom);
        List<HospitalVisit> olderVisits = visitController.getOlderVisits(mrn, messageDateTime);
        if (olderVisits.isEmpty()) {
            logger.warn("No existing visits for DeletePersonMessage message: {}", msg);
            return;
        }
        patientLocationController.deleteLocationVisits(olderVisits, messageDateTime, storedFrom);
        deletionController.deleteVisitsAndDependentEntities(olderVisits, messageDateTime, storedFrom);
    }

    /**
     * Move a visit from a previous MRN to the current MRN.
     * @param msg        MoveVisitInformation
     * @param storedFrom time that emap-core started processing the message.
     * @throws EmapOperationMessageProcessingException if message can't be processed.
     */
    @Transactional
    public void moveVisitInformation(MoveVisitInformation msg, Instant storedFrom) throws EmapOperationMessageProcessingException {
        Instant messageDateTime = msg.bestGuessAtValidFrom();
        Mrn previousMrn = personController.getOrCreateMrn(
                msg.getPreviousMrn(), msg.getPreviousNhsNumber(), msg.getSourceSystem(), messageDateTime, storedFrom);
        Mrn currentMrn = processPersonLevel(msg, storedFrom, messageDateTime);
        visitController.moveVisitInformation(msg, storedFrom, previousMrn, currentMrn);
    }

    /**
     * Change the MRN string and NHS number for a patient.
     * @param msg        ChangePatientIdentifiers
     * @param storedFrom time that emap-core started processing the message.
     * @throws EmapOperationMessageProcessingException if message can't be processed.
     */
    @Transactional
    public void changePatientIdentifiers(ChangePatientIdentifiers msg, Instant storedFrom) throws EmapOperationMessageProcessingException {
        Instant messageDateTime = msg.bestGuessAtValidFrom();
        personController.updatePatientIdentifiersOrMerge(msg, messageDateTime, storedFrom);
    }

    /**
     * Swap the locations of two patient's encounters.
     * @param msg        swap locations
     * @param storedFrom time that emap-core started processing the message.
     * @throws EmapOperationMessageProcessingException if message can't be processed.
     */
    @Transactional
    public void swapLocations(SwapLocations msg, Instant storedFrom) throws EmapOperationMessageProcessingException {
        Instant messageDateTime = msg.bestGuessAtValidFrom();

        // process first visit
        Mrn mrnA = personController.getOrCreateMrn(
                msg.getMrn(), msg.getNhsNumber(), msg.getSourceSystem(), msg.getRecordedDateTime(), storedFrom);
        personController.updateOrCreateDemographic(mrnA, msg, messageDateTime, storedFrom);
        HospitalVisit visitA = visitController.updateOrCreateHospitalVisit(msg, storedFrom, mrnA);

        // get the other visit
        Mrn mrnB = personController.getOrCreateMrn(
                msg.getOtherMrn(), msg.getOtherNhsNumber(), msg.getSourceSystem(), msg.getRecordedDateTime(), storedFrom);
        HospitalVisit visitB = visitController.getOrCreateMinimalHospitalVisit(
                msg.getOtherVisitNumber(), mrnB, msg.getSourceSystem(), messageDateTime, storedFrom);

        // swap locations
        patientLocationController.swapLocations(visitA, visitB, msg, storedFrom);
    }

    /**
     * Process a pending ADT message.
     * <p>
     * Adds in all patient level information and a HospitalVisit so that a PlannedMovement can be created.
     * @param msg        pending adt message
     * @param storedFrom time that emap core started processing the message
     * @throws RequiredDataMissingException if the visit number is missing
     */
    @Transactional
    public void processPendingAdt(PendingTransfer msg, Instant storedFrom) throws RequiredDataMissingException {
        Instant validFrom = msg.bestGuessAtValidFrom();
        HospitalVisit visit = processPersonAndVisit(msg, storedFrom, validFrom);
        pendingAdtController.processMsg(visit, msg, validFrom, storedFrom);
    }

    /**
     * Process a cancellation of pending ADT message.
     * <p>
     * Adds in all patient level information and a HospitalVisit so that a PlannedMovement can be created.
     * @param msg        pending adt message
     * @param storedFrom time that emap core started processing the message
     * @throws RequiredDataMissingException if the visit number is missing
     */
    @Transactional
    public void processPendingAdt(CancelPendingTransfer msg, Instant storedFrom) throws RequiredDataMissingException {
        Instant validFrom = msg.bestGuessAtValidFrom();
        HospitalVisit visit = processPersonAndVisit(msg, storedFrom, validFrom);
        pendingAdtController.processMsg(visit, msg, validFrom, storedFrom);
    }

    /**
     * Process an update subspeciality message.
     * <p>
     * Updates the sub speciality in the hospital visit table
     * @param msg        change sub speciality adt message
     * @param storedFrom time that emap core started processing the message
     * @throws RequiredDataMissingException if the visit number is missing
     */
    @Transactional
    public void processUpdateSubSpeciality(UpdateSubSpeciality msg, Instant storedFrom) throws RequiredDataMissingException {
        Instant validFrom = msg.bestGuessAtValidFrom();
        HospitalVisit visit = processPersonAndVisit(msg, storedFrom, validFrom);
        // patientLocationController.processVisitLocation(visit, msg, storedFrom);
        pendingAdtController.processMsg(visit, msg, validFrom, storedFrom);
    }

}
