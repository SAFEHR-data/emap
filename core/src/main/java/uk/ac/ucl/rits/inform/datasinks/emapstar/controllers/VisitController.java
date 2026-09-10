package uk.ac.ucl.rits.inform.datasinks.emapstar.controllers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import uk.ac.ucl.rits.inform.datasinks.emapstar.DataSources;
import uk.ac.ucl.rits.inform.datasinks.emapstar.RowState;
import uk.ac.ucl.rits.inform.datasinks.emapstar.exceptions.IncompatibleDatabaseStateException;
import uk.ac.ucl.rits.inform.datasinks.emapstar.exceptions.RequiredDataMissingException;
import uk.ac.ucl.rits.inform.datasinks.emapstar.repos.HospitalVisitAuditRepository;
import uk.ac.ucl.rits.inform.datasinks.emapstar.repos.HospitalVisitRepository;
import uk.ac.ucl.rits.inform.informdb.identity.HospitalVisit;
import uk.ac.ucl.rits.inform.informdb.identity.HospitalVisitAudit;
import uk.ac.ucl.rits.inform.informdb.identity.Mrn;
import uk.ac.ucl.rits.inform.interchange.adt.AdmissionDateTime;
import uk.ac.ucl.rits.inform.interchange.adt.AdmitPatient;
import uk.ac.ucl.rits.inform.interchange.adt.AdtCancellation;
import uk.ac.ucl.rits.inform.interchange.adt.AdtMessage;
import uk.ac.ucl.rits.inform.interchange.adt.CancelAdmitPatient;
import uk.ac.ucl.rits.inform.interchange.adt.CancelDischargePatient;
import uk.ac.ucl.rits.inform.interchange.adt.DischargePatient;
import uk.ac.ucl.rits.inform.interchange.adt.ImpliedAdtMessage;
import uk.ac.ucl.rits.inform.interchange.adt.MoveVisitInformation;
import uk.ac.ucl.rits.inform.interchange.adt.RegisterPatient;
import uk.ac.ucl.rits.inform.interchange.adt.UpdatePatientInfo;

import java.time.Instant;
import java.util.List;

/**
 * Interactions with visits.
 * @author Stef Piatek
 */
@Component
public class VisitController {
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final HospitalVisitRepository hospitalVisitRepo;
    private final HospitalVisitAuditRepository hospitalVisitAuditRepo;

    /**
     * @param hospitalVisitRepo      repository for HospitalVisit
     * @param hospitalVisitAuditRepo repository for HospitalVisitAudit
     */
    public VisitController(HospitalVisitRepository hospitalVisitRepo, HospitalVisitAuditRepository hospitalVisitAuditRepo) {
        this.hospitalVisitRepo = hospitalVisitRepo;
        this.hospitalVisitAuditRepo = hospitalVisitAuditRepo;
    }

    /**
     * Get or create hospital visit, should be used for non-ADT source.
     * Will create a minimum hospital visit and save it it can't match one by the encounter string.
     * @param encounter       encounter number
     * @param mrn             Mrn
     * @param sourceSystem    source system
     * @param messageDateTime date time of the message
     * @param storedFrom      when the message has been read by emap core
     * @return Hospital visit from database or minimal hospital visit
     * @throws RequiredDataMissingException if no encounter in message
     */
    public HospitalVisit getOrCreateMinimalHospitalVisit(
            final String encounter, final Mrn mrn, final String sourceSystem, final Instant messageDateTime, final Instant storedFrom
    ) throws RequiredDataMissingException {
        RowState<HospitalVisit, HospitalVisitAudit> visit = getOrCreateHospitalVisit(encounter, mrn, sourceSystem, messageDateTime, storedFrom);
        if (visit.isEntityCreated()) {
            logger.debug("Minimal encounter created. encounter: {}, mrn: {}", encounter, mrn);
            hospitalVisitRepo.save(visit.getEntity());
        }
        return visit.getEntity();
    }

    /**
     * Get or create minimal hospital visit, and update whether it was created.
     * @param encounter       encounter number
     * @param mrn             Mrn
     * @param sourceSystem    source system
     * @param messageDateTime date time of the message
     * @param storedFrom      when the message has been read by emap core
     * @return existing visit or created minimal visit
     * @throws RequiredDataMissingException if no encounter in message
     */
    private RowState<HospitalVisit, HospitalVisitAudit> getOrCreateHospitalVisit(
            final String encounter, final Mrn mrn, final String sourceSystem, final Instant messageDateTime,
            final Instant storedFrom) throws RequiredDataMissingException {
        if (encounter == null || encounter.isEmpty()) {
            throw new RequiredDataMissingException("No encounter in message");
        }
        logger.debug("Getting or create Hospital Visit: mrn {}, encounter {}", mrn, encounter);
        return hospitalVisitRepo.findByEncounter(encounter)
                .map(visit -> new RowState<>(visit, messageDateTime, storedFrom, false))
                .orElseGet(() -> createHospitalVisit(encounter, mrn, sourceSystem, messageDateTime, storedFrom));
    }


    /**
     * Create minimal hospital visit.
     * @param encounter       encounter number
     * @param mrn             Mrn
     * @param sourceSystem    source system
     * @param messageDateTime date time of the message
     * @param storedFrom      when the message has been read by emap core
     * @return new hospital visit
     */
    private RowState<HospitalVisit, HospitalVisitAudit> createHospitalVisit(
            final String encounter, Mrn mrn, final String sourceSystem, final Instant messageDateTime, final Instant storedFrom) {
        HospitalVisit visit = new HospitalVisit();
        visit.setMrnId(mrn);
        visit.setEncounter(encounter);
        visit.setSourceSystem(sourceSystem);
        visit.setStoredFrom(storedFrom);
        visit.setValidFrom(messageDateTime);
        return new RowState<>(visit, messageDateTime, storedFrom, true);
    }

    /**
     * Process information about hospital visits, saving any changes to the database.
     * @param msg        adt message
     * @param storedFrom time that emap-core started processing the message.
     * @param mrn        mrn
     * @return hospital visit, may be null if an UpdatePatientInfo or ImpliedAdtMessage message doesn't have any encounter information.
     * @throws RequiredDataMissingException if an adt message has no visit number set and is not an UpdatePatientInfo message
     */
    @Transactional
    public HospitalVisit updateOrCreateHospitalVisit(
            final AdtMessage msg, final Instant storedFrom, final Mrn mrn) throws RequiredDataMissingException {
        if (msg instanceof UpdatePatientInfo) {
            logger.debug("UpdatePatientInfo does not create and encounter");
            return null;
        }

        if (msg.getVisitNumber() == null || msg.getVisitNumber().isEmpty()) {
            if (msg instanceof ImpliedAdtMessage) {
                logger.debug("ImpliedAdtMessage had no encounter information");
                return null;
            }
            throw new RequiredDataMissingException(String.format("ADT message doesn't have a visit number: %s", msg));
        }
        Instant validFrom = msg.bestGuessAtValidFrom();
        RowState<HospitalVisit, HospitalVisitAudit> visitState = getOrCreateHospitalVisit(
                msg.getVisitNumber(), mrn, msg.getSourceSystem(), validFrom, storedFrom);
        addAdmissionType(msg, visitState);
        if (visitShouldBeUpdated(validFrom, msg.getSourceSystem(), visitState)) {
            updateGenericData(msg, visitState);

            // process message based on the class type
            if (msg instanceof CancelDischargePatient) {
                removeDischargeInformation((AdtCancellation) msg, visitState);
            } else if (msg instanceof CancelAdmitPatient) {
                removeAdmissionInformation((AdtCancellation) msg, visitState);
            }
        }
        addPresentationAdmissionOrDischargeTime(msg, visitState);
        visitState.saveEntityOrAuditLogIfRequired(hospitalVisitRepo, hospitalVisitAuditRepo);
        return visitState.getEntity();
    }

    /**
     * For mid-stream running, add in presentation and admission time if these have been missed, regardless of the valid from date.
     * Common with A08 messages without an event occurred date time, this causes a later valid from date to be set and valid updates will be skipped.
     * @param msg        adt message
     * @param visitState visit wrapped in state class
     */
    private void addPresentationAdmissionOrDischargeTime(final AdtMessage msg, RowState<HospitalVisit, HospitalVisitAudit> visitState) {
        if (!DataSources.isTrusted(msg.getSourceSystem())) {
            return;
        }

        // Add admission date time from any message that has it if it doesn't exist already, always update from an AdmitPatient message
        if (msg instanceof AdmissionDateTime && visitState.getEntity().getAdmissionDatetime() == null || msg instanceof AdmitPatient) {
            addAdmissionDateTime((AdmissionDateTime) msg, visitState);
        } else if (msg instanceof RegisterPatient) {
            addRegistrationInformation((RegisterPatient) msg, visitState);
        }
        // Process discharge separately because it can add an AdmissionDateTime if it doesn't already exist
        if (msg instanceof DischargePatient) {
            addDischargeInformation((DischargePatient) msg, visitState);
        }
    }

    /**
     * Update visit if message is from a trusted source update if newer or if database source isn't trusted.
     * Otherwise only update if if is newly created.
     * @param messageDateTime date time of the message
     * @param messageSource   Source system from the message
     * @param visitState      visit wrapped in state class
     * @return true if the visit should be updated
     */
    private boolean visitShouldBeUpdated(
            final Instant messageDateTime, final String messageSource, final RowState<HospitalVisit, HospitalVisitAudit> visitState) {
        // always update if a message is created
        if (visitState.isEntityCreated()) {
            return true;
        }
        HospitalVisit visit = visitState.getEntity();
        // if message source is trusted and (entity source system is untrusted or message is newer)
        return DataSources.isTrusted(messageSource)
                && (!DataSources.isTrusted(visit.getSourceSystem()) || !visit.getValidFrom().isAfter(messageDateTime));
    }

    /**
     * Update visit with generic ADT information.
     * @param msg        adt message
     * @param visitState visit wrapped in state class
     */
    private void updateGenericData(final AdtMessage msg, RowState<HospitalVisit, HospitalVisitAudit> visitState) {
        HospitalVisit visit = visitState.getEntity();
        visitState.assignInterchangeValue(msg.getPatientClass(), visit.getPatientClass(), visit::setPatientClass);
        visitState.assignInterchangeValue(msg.getModeOfArrival(), visit.getArrivalMethod(), visit::setArrivalMethod);
        visitState.assignIfDifferent(msg.getSourceSystem(), visit.getSourceSystem(), visit::setSourceSystem);
    }

    /**
     * Add admission date time.
     * @param msg        AdmissionDateTime
     * @param visitState visit wrapped in state class
     */
    private void addAdmissionDateTime(final AdmissionDateTime msg, RowState<HospitalVisit, HospitalVisitAudit> visitState) {
        HospitalVisit visit = visitState.getEntity();
        visitState.assignInterchangeValue(msg.getAdmissionDateTime(), visit.getAdmissionDatetime(), visit::setAdmissionDatetime);
    }

    /**
     * Add admission type.
     * @param msg        AdmissionDateTime
     * @param visitState visit wrapped in state class
     */
    private void addAdmissionType(final AdtMessage msg, RowState<HospitalVisit, HospitalVisitAudit> visitState) {
        HospitalVisit visit = visitState.getEntity();
        visitState.assignInterchangeValue(msg.getAdmissionType(), visit.getAdmissionType(), visit::setAdmissionType);
    }


    /**
     * Delete admission specific information.
     * @param msg        cancellation message
     * @param visitState visit wrapped in state class
     */
    private void removeAdmissionInformation(final AdtCancellation msg, RowState<HospitalVisit, HospitalVisitAudit> visitState) {
        HospitalVisit visit = visitState.getEntity();
        visitState.removeIfExists(visit.getAdmissionDatetime(), visit::setAdmissionDatetime, msg.getCancelledDateTime());
    }

    /**
     * Add registration specific information.
     * @param msg        adt message
     * @param visitState visit wrapped in state class
     */
    private void addRegistrationInformation(final RegisterPatient msg, RowState<HospitalVisit, HospitalVisitAudit> visitState) {
        HospitalVisit visit = visitState.getEntity();
        visitState.assignInterchangeValue(msg.getPresentationDateTime(), visit.getPresentationDatetime(), visit::setPresentationDatetime);
    }

    /**
     * Add discharge specific information.
     * If no value for admission time, add this in from the discharge message.
     * @param msg        adt message
     * @param visitState visit wrapped in state class
     */
    private void addDischargeInformation(final DischargePatient msg, RowState<HospitalVisit, HospitalVisitAudit> visitState) {
        HospitalVisit visit = visitState.getEntity();
        visitState.assignIfDifferent(msg.getDischargeDateTime(), visit.getDischargeDatetime(), visit::setDischargeDatetime);
        visitState.assignIfDifferent(msg.getDischargeDisposition(), visit.getDischargeDisposition(), visit::setDischargeDisposition);
        visitState.assignIfDifferent(msg.getDischargeLocation(), visit.getDischargeDestination(), visit::setDischargeDestination);

        // If started mid-stream, no admission information so add this in on discharge
        if (visit.getAdmissionDatetime() == null && !msg.getAdmissionDateTime().isUnknown()) {
            visitState.assignInterchangeValue(msg.getAdmissionDateTime(), visit.getAdmissionDatetime(), visit::setAdmissionDatetime);
        }
    }

    /**
     * Remove discharge specific information.
     * @param msg        cancellation message
     * @param visitState visit wrapped in state class
     */
    private void removeDischargeInformation(final AdtCancellation msg, RowState<HospitalVisit, HospitalVisitAudit> visitState) {
        HospitalVisit visit = visitState.getEntity();
        visitState.removeIfExists(visit.getDischargeDatetime(), visit::setDischargeDatetime, msg.getCancelledDateTime());
        visitState.removeIfExists(visit.getDischargeDisposition(), visit::setDischargeDisposition, msg.getCancelledDateTime());
        visitState.removeIfExists(visit.getDischargeDestination(), visit::setDischargeDestination, msg.getCancelledDateTime());

    }


    public List<HospitalVisit> getOlderVisits(Mrn mrn, Instant messageDateTime) {
        return hospitalVisitRepo.findAllByMrnIdAndValidFromIsLessThanEqual(mrn, messageDateTime);
    }

    /**
     * Move visit information from previous MRN to current MRN.
     * @param msg         MoveVisitInformation message
     * @param storedFrom  time that emap-core started processing the message.
     * @param previousMrn previous MRN
     * @param currentMrn  new MRN that the encounter should be linked with
     * @return hospital visit
     * @throws RequiredDataMissingException       if message is missing required data
     * @throws IncompatibleDatabaseStateException If the message will not have an effect or the new encounter already exists
     */
    @Transactional
    public HospitalVisit moveVisitInformation(MoveVisitInformation msg, Instant storedFrom, Mrn previousMrn, Mrn currentMrn)
            throws RequiredDataMissingException, IncompatibleDatabaseStateException {
        if (msg.getPreviousVisitNumber().equals(msg.getVisitNumber()) && previousMrn.equals(currentMrn)) {
            throw new IncompatibleDatabaseStateException(String.format("MoveVisitInformation will not change the MRN or the visit number: %s", msg));
        }
        if (isVisitNumberChangesAndFinalEncounterAlreadyExists(msg)) {
            throw new IncompatibleDatabaseStateException(String.format("MoveVisitInformation where new encounter already exists : %s", msg));
        }

        Instant validFrom = msg.bestGuessAtValidFrom();
        RowState<HospitalVisit, HospitalVisitAudit> visitState = getOrCreateHospitalVisit(
                msg.getPreviousVisitNumber(), previousMrn, msg.getSourceSystem(), validFrom, storedFrom);

        if (visitShouldBeUpdated(validFrom, msg.getSourceSystem(), visitState)) {
            updateGenericData(msg, visitState);
            // move the encounter and MRN to the correct value
            HospitalVisit visit = visitState.getEntity();
            visitState.assignIfDifferent(msg.getPreviousVisitNumber(), visit.getEncounter(), visit::setEncounter);
            visitState.assignIfDifferent(currentMrn, visit.getMrnId(), visit::setMrnId);
        }
        visitState.saveEntityOrAuditLogIfRequired(hospitalVisitRepo, hospitalVisitAuditRepo);
        return visitState.getEntity();
    }

    /**
     * @param msg MoveVisitInformation
     * @return true if the message visit number changes and the final encounter already exists
     */
    private boolean isVisitNumberChangesAndFinalEncounterAlreadyExists(MoveVisitInformation msg) {
        return !msg.getPreviousVisitNumber().equals(msg.getVisitNumber()) && hospitalVisitRepo.findByEncounter(msg.getVisitNumber()).isPresent();
    }

    /**
     * Deletes visit and updates corresponding audit table.
     * @param visit             List of hospital visits
     * @param invalidationTime  Time of the delete information message
     * @param deletionTime      time that emap-core started processing the message.
     */
    public void deleteVisit(HospitalVisit visit, Instant invalidationTime, Instant deletionTime) {
        hospitalVisitAuditRepo.save(visit.createAuditEntity(invalidationTime, deletionTime));
        hospitalVisitRepo.delete(visit);
    }

}
