package uk.ac.ucl.rits.inform.datasinks.emapstar.controllers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import uk.ac.ucl.rits.inform.datasinks.emapstar.RowState;
import uk.ac.ucl.rits.inform.datasinks.emapstar.exceptions.RequiredDataMissingException;
import uk.ac.ucl.rits.inform.datasinks.emapstar.repos.ConditionTypeRepository;
import uk.ac.ucl.rits.inform.datasinks.emapstar.repos.PatientConditionAuditRepository;
import uk.ac.ucl.rits.inform.datasinks.emapstar.repos.PatientConditionRepository;
import uk.ac.ucl.rits.inform.informdb.conditions.ConditionType;
import uk.ac.ucl.rits.inform.informdb.conditions.PatientCondition;
import uk.ac.ucl.rits.inform.informdb.conditions.PatientConditionAudit;
import uk.ac.ucl.rits.inform.informdb.identity.Mrn;
import uk.ac.ucl.rits.inform.interchange.EmapOperationMessageProcessingException;
import uk.ac.ucl.rits.inform.interchange.PatientInfection;

import java.time.Instant;
import java.util.Optional;


/**
 * Interactions with patient conditions.
 * @author Anika Cawthorn
 * @author Stef Piatek
 */
@Component
public class PatientConditionController {
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final PatientConditionRepository patientConditionRepo;
    private final ConditionTypeRepository conditionTypeRepo;
    private final PatientConditionAuditRepository patientConditionAuditRepo;

    private enum PatientConditionType {
        PATIENT_INFECTION
    }

    /**
     * Setting repositories holding information on patient conditions.
     * @param patientConditionRepo      autowired PatientConditionRepository
     * @param patientConditionAuditRepo autowired PatientConditionAuditRepository
     * @param conditionTypeRepo         autowired ConditionTypeRepository
     */
    public PatientConditionController(
            PatientConditionRepository patientConditionRepo, PatientConditionAuditRepository patientConditionAuditRepo,
            ConditionTypeRepository conditionTypeRepo) {
        this.patientConditionRepo = patientConditionRepo;
        this.patientConditionAuditRepo = patientConditionAuditRepo;
        this.conditionTypeRepo = conditionTypeRepo;
    }

    /**
     * Get existing condition type or create and save minimal condition type.
     * @param type            Condition Type
     * @param typeName        name of the individual condition within the type
     * @param updatedDateTime when the condition information is valid from
     * @param storedFrom      when patient infection information is stored from
     * @return ConditionType
     */
    @Cacheable(value = "conditionType", key = "{#dataType, #typeName}")
    public ConditionType getOrCreateConditionType(
            PatientConditionType type, String typeName, Instant updatedDateTime, Instant storedFrom) {
        return conditionTypeRepo
                .findByDataTypeAndName(type.toString(), typeName)
                .orElseGet(() -> {
                    ConditionType conditionType = new ConditionType(type.toString(), typeName, updatedDateTime, storedFrom);
                    logger.debug("Created new {}", conditionType);
                    return conditionTypeRepo.save(conditionType);
                });
    }

    /**
     * Process patient condition message.
     * @param msg        message
     * @param mrn        patient id
     * @param storedFrom valid from in database
     * @throws EmapOperationMessageProcessingException if message can't be processed.
     */
    @Transactional
    public void processMessage(final PatientInfection msg, Mrn mrn, final Instant storedFrom)
            throws EmapOperationMessageProcessingException {
        ConditionType conditionType = getOrCreateConditionType(
                PatientConditionType.PATIENT_INFECTION, msg.getInfection(), msg.getUpdatedDateTime(), storedFrom);
        if ("hoover".equals(msg.getSourceSystem())) {
            // we can't trust the hl7 feed so when we find a hoover patient infection, delete the previous ones
            logger.debug("Deleting all {} infections before {}", conditionType.getName(), msg.getUpdatedDateTime());
            patientConditionRepo.deleteAllByValidFromBeforeAndInternalIdIsNullAndConditionTypeId(msg.getUpdatedDateTime(), conditionType);
        }
        RowState<PatientCondition, PatientConditionAudit> patientCondition = getOrCreatePatientCondition(msg, mrn, conditionType, storedFrom);

        if (messageShouldBeUpdated(msg, patientCondition)) {
            updatePatientCondition(msg, patientCondition);
        }

        patientCondition.saveEntityOrAuditLogIfRequired(patientConditionRepo, patientConditionAuditRepo);
    }

    /**
     * Get or create existing patient condition entity.
     * @param msg           patient infection message
     * @param mrn           patient identifier
     * @param conditionType condition type referred to in message
     * @param storedFrom    time that emap-core started processing the message
     * @return observation entity wrapped in RowState
     */
    private RowState<PatientCondition, PatientConditionAudit> getOrCreatePatientCondition(
            PatientInfection msg, Mrn mrn, ConditionType conditionType, Instant storedFrom) throws RequiredDataMissingException {
        Optional<PatientCondition> patientCondition;
        final Long epicInfectionId;
        switch (msg.getSourceSystem()) {
            case "EPIC":
                epicInfectionId = null;
                patientCondition = patientConditionRepo
                        .findByMrnIdAndConditionTypeIdAndAddedDateTime(mrn, conditionType, msg.getInfectionAdded());
                break;
            case "hoover":
                if (msg.getEpicInfectionId().isUnknown()) {
                    throw new RequiredDataMissingException("No patientInfectionId from hoover");
                }
                epicInfectionId = msg.getEpicInfectionId().get();
                patientCondition = patientConditionRepo.findByConditionTypeIdAndInternalId(conditionType, epicInfectionId);
                break;
            default:
                throw new RequiredDataMissingException(String.format("'%s' is not a recognised source system", msg.getSourceSystem()));
        }

        return patientCondition
                .map(obs -> new RowState<>(obs, msg.getUpdatedDateTime(), storedFrom, false))
                .orElseGet(() -> createMinimalPatientCondition(
                        epicInfectionId, mrn, conditionType, msg.getInfectionAdded(), msg.getUpdatedDateTime(), storedFrom));
    }

    /**
     * Create minimal patient condition wrapped in RowState.
     * @param epicConditionId internal epic Id for condition
     * @param mrn             patient identifier
     * @param conditionType   condition type
     * @param conditionAdded  condition added at
     * @param validFrom       hospital time that the data is true from
     * @param storedFrom      time that emap-core started processing the message
     * @return minimal patient condition wrapped in RowState
     */
    private RowState<PatientCondition, PatientConditionAudit> createMinimalPatientCondition(
            Long epicConditionId, Mrn mrn, ConditionType conditionType, Instant conditionAdded, Instant validFrom, Instant storedFrom) {

        PatientCondition patientCondition = new PatientCondition(epicConditionId, conditionType, mrn, conditionAdded);
        return new RowState<>(patientCondition, validFrom, storedFrom, true);
    }

    /**
     * Update message if observation has been created, or the message updated time is >= entity validFrom.
     * @param msg           patient infection
     * @param conditionDate row state of condition
     * @return true if message should be updated
     */
    private boolean messageShouldBeUpdated(PatientInfection msg, RowState<PatientCondition, PatientConditionAudit> conditionDate) {
        return conditionDate.isEntityCreated() || !msg.getUpdatedDateTime().isBefore(conditionDate.getEntity().getConditionTypeId().getValidFrom());
    }

    /**
     * Update patient condition from patient infection message.
     * @param msg            patient infection message
     * @param conditionState patient condition entity to update
     */
    private void updatePatientCondition(PatientInfection msg, RowState<PatientCondition, PatientConditionAudit> conditionState) {
        PatientCondition condition = conditionState.getEntity();
        conditionState.assignInterchangeValue(msg.getComment(), condition.getComment(), condition::setComment);
        conditionState.assignInterchangeValue(msg.getStatus(), condition.getStatus(), condition::setStatus);
        conditionState.assignInterchangeValue(msg.getInfectionResolved(), condition.getResolutionDateTime(), condition::setResolutionDateTime);
        conditionState.assignInterchangeValue(msg.getInfectionOnset(), condition.getOnsetDate(), condition::setOnsetDate);
    }
}
