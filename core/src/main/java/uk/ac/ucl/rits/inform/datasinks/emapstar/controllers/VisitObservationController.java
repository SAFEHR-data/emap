package uk.ac.ucl.rits.inform.datasinks.emapstar.controllers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import uk.ac.ucl.rits.inform.datasinks.emapstar.RowState;
import uk.ac.ucl.rits.inform.datasinks.emapstar.exceptions.RequiredDataMissingException;
import uk.ac.ucl.rits.inform.datasinks.emapstar.repos.visit_observations.VisitObservationAuditRepository;
import uk.ac.ucl.rits.inform.datasinks.emapstar.repos.visit_observations.VisitObservationRepository;
import uk.ac.ucl.rits.inform.datasinks.emapstar.repos.visit_observations.VisitObservationTypeAuditRepository;
import uk.ac.ucl.rits.inform.datasinks.emapstar.repos.visit_observations.VisitObservationTypeRepository;
import uk.ac.ucl.rits.inform.informdb.identity.HospitalVisit;
import uk.ac.ucl.rits.inform.informdb.visit_recordings.VisitObservation;
import uk.ac.ucl.rits.inform.informdb.visit_recordings.VisitObservationAudit;
import uk.ac.ucl.rits.inform.informdb.visit_recordings.VisitObservationType;
import uk.ac.ucl.rits.inform.informdb.visit_recordings.VisitObservationTypeAudit;
import uk.ac.ucl.rits.inform.interchange.visit_observations.Flowsheet;
import uk.ac.ucl.rits.inform.interchange.visit_observations.FlowsheetMetadata;
import uk.ac.ucl.rits.inform.interchange.visit_observations.WaveformMessage;

import javax.annotation.Resource;
import java.time.Instant;

/**
 * Interactions with observation visits.
 * @author Stef Piatek
 * @author Anika Cawthorn
 */
@Component
public class VisitObservationController {

    @Resource
    private VisitObservationCache cache;
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final VisitObservationRepository visitObservationRepo;
    private final VisitObservationAuditRepository visitObservationAuditRepo;
    private final VisitObservationTypeRepository visitObservationTypeRepo;
    private final VisitObservationTypeAuditRepository visitObservationTypeAuditRepo;

    /**
     * @param visitObservationRepo          autowired
     * @param visitObservationAuditRepo     autowired
     * @param visitObservationTypeRepo      autowired
     * @param visitObservationTypeAuditRepo autowired
     */
    public VisitObservationController(
            VisitObservationRepository visitObservationRepo, VisitObservationAuditRepository visitObservationAuditRepo,
            VisitObservationTypeRepository visitObservationTypeRepo, VisitObservationTypeAuditRepository visitObservationTypeAuditRepo) {
        this.visitObservationRepo = visitObservationRepo;
        this.visitObservationAuditRepo = visitObservationAuditRepo;
        this.visitObservationTypeRepo = visitObservationTypeRepo;
        this.visitObservationTypeAuditRepo = visitObservationTypeAuditRepo;
    }

    /**
     * Process metadata, clearing existing cache for all visit observations.
     * There are two different types of metadata: i) containing the mapping between an interfaceId and idInApplication and
     * ii) containing lots of naming data for the particular VisitObservationType. This function decides what kind of
     * metadata is handled and how it should therefore be processed.
     * @param msg        flowsheet metadata
     * @param storedFrom time that star started processing the message
     * @throws java.util.NoSuchElementException if the VisitObservationTypes that a mapping message is referring to cannot be found
     * @throws RequiredDataMissingException     if required data is missing
     */
    @Transactional
    @CacheEvict(value = "visitObservationType", allEntries = true)
    public void processMetadata(FlowsheetMetadata msg, Instant storedFrom) throws RequiredDataMissingException {
        if (msg.getInterfaceId() == null && msg.getFlowsheetId() == null) {
            throw new RequiredDataMissingException("Both identifiers cannot be null");
        }
        // if both IDs are present, it's a mapping metadata message (as opposed to data for observation type)
        if (msg.getInterfaceId() != null && msg.getFlowsheetId() != null) {
            processMappingMessage(msg, storedFrom);
        } else {
            RowState<VisitObservationType, VisitObservationTypeAudit> typeState = getOrCreateObservationTypeState(
                    msg.getInterfaceId(), msg.getFlowsheetId(), msg.getSourceObservationType(), msg.getLastUpdatedInstant(), storedFrom);
            VisitObservationType observationType = typeState.getEntity();
            // Update metadata with usable information
            Instant messageValidFrom = msg.getLastUpdatedInstant();
            Instant entityValidFrom = observationType.getValidFrom();
            typeState.assignIfCurrentlyNullOrNewerAndDifferent(
                    msg.getName(), observationType.getName(), observationType::setName, messageValidFrom, entityValidFrom);
            typeState.assignIfCurrentlyNullOrNewerAndDifferent(
                    msg.getDisplayName(), observationType.getDisplayName(), observationType::setDisplayName, messageValidFrom, entityValidFrom);
            typeState.assignIfCurrentlyNullOrNewerAndDifferent(
                    msg.getDescription(), observationType.getDescription(), observationType::setDescription, messageValidFrom, entityValidFrom);
            typeState.assignIfCurrentlyNullOrNewerAndDifferent(
                    msg.getValueType(), observationType.getPrimaryDataType(), observationType::setPrimaryDataType, messageValidFrom, entityValidFrom);
            typeState.assignIfCurrentlyNullOrNewerAndDifferent(
                    msg.getCreationInstant(), observationType.getCreationDatetime(), observationType::setCreationDatetime,
                    messageValidFrom, entityValidFrom);

            typeState.saveEntityOrAuditLogIfRequired(visitObservationTypeRepo, visitObservationTypeAuditRepo);
        }
    }

    /**
     * Create, update or delete a flowsheet, saving the visit observation to the cache.
     * Will also create a new VisitObservationType if it doesn't already exist.
     * @param msg        flowsheet
     * @param visit      hospital visit
     * @param storedFrom time that emap-core started processing the message
     * @throws RequiredDataMissingException if isNumericType is not set
     */
    @Transactional
    public void processFlowsheet(Flowsheet msg, HospitalVisit visit, Instant storedFrom) throws RequiredDataMissingException {
        if (msg.getValueType() == null) {
            throw new RequiredDataMissingException("Flowsheet DataType not set");
        }

        var validFrom = msg.getLastUpdatedInstant();
        VisitObservationType observationType = cache.getOrCreatePersistedObservationType(msg.getInterfaceId(),
                msg.getFlowsheetId(), msg.getSourceObservationType(), validFrom, storedFrom);

        RowState<VisitObservation, VisitObservationAudit> flowsheetState = getOrCreateFlowsheet(msg, visit, observationType, storedFrom);
        if (flowsheetState.messageShouldBeUpdated(msg.getLastUpdatedInstant())) {
            updateVisitObservation(msg, flowsheetState);
            flowsheetState.saveEntityOrAuditLogIfRequired(visitObservationRepo, visitObservationAuditRepo);
            updateDataFlagsAndSaveObservationType(msg, observationType, validFrom, storedFrom);
        }
    }

    /**
     * Process the parts of a waveform message that relate to its visit observation type.
     * @param msg waveform msg from which to extract VisitObservationType data
     * @param storedFrom stored from
     * @return new or existing VisitObservationType
     */
    public VisitObservationType getOrCreateFromWaveform(WaveformMessage msg, Instant storedFrom) {
        VisitObservationType observationType = cache.getOrCreatePersistedObservationType(
                msg.getSourceStreamId(), msg.getSourceStreamId(), "waveform", msg.getObservationTime(), storedFrom);
        observationType.setName(msg.getMappedStreamDescription());
        observationType.setIsRealTime(true);
        // Might want to consider updating/auditing if the name/description changes. See Issue #71
        return observationType;
    }

    /**
     * If mapping between internal id and interface id already exists, nothing (?) needs to change.
     * @param interfaceId     Identifier of observation type.
     * @param idInApplication IdInApplication of observation type.
     * @return True if mapping between interfaceId and idInApplication already exists, otherwise false.
     */
    public boolean mappingExists(String interfaceId, String idInApplication) {
        return visitObservationTypeRepo.findByInterfaceIdAndIdInApplication(interfaceId, idInApplication).isPresent();
    }

    /**
     * Deletes VisitObservationType that was created in the absence of mapping information. Once mapping information is
     * present, the metadata VisitObservationType will be updated instead and the key information replaced respectively.
     * @param visitObservationType VisitObservationType to be deleted as object for repository deletion
     * @param validFrom            Datetime from which the VisitObservationType was deleted
     * @param storedFrom           Datetime when the information was first held in Star
     */
    public void deleteVisitObservationType(VisitObservationType visitObservationType, Instant validFrom, Instant storedFrom) {
        visitObservationTypeAuditRepo.save(new VisitObservationTypeAudit(visitObservationType, validFrom, storedFrom));
        logger.debug("Deleting LocationVisit: {}", visitObservationType);
        visitObservationTypeRepo.delete(visitObservationType);
    }

    /**
     * There are two different types of metadata: i) containing the mapping between an interfaceId and idInApplication and
     * ii) containing lots of naming data for the particular VisitObservationType.
     * @param msg        Flowsheet metadata message containing mapping information
     * @param storedFrom When this information was first processed in Star.
     */
    private void processMappingMessage(FlowsheetMetadata msg, Instant storedFrom) {
        if (mappingExists(msg.getInterfaceId(), msg.getFlowsheetId())) {
            return;
        }
        RowState<VisitObservationType, VisitObservationTypeAudit> votCaboodleState = getTypeStateOrNull(null, msg.getFlowsheetId(),
                msg.getLastUpdatedInstant(), msg.getCreationInstant(), msg.getSourceObservationType());
        RowState<VisitObservationType, VisitObservationTypeAudit> votEpicState = getTypeStateOrNull(msg.getInterfaceId(), null,
                msg.getLastUpdatedInstant(), msg.getCreationInstant(), msg.getSourceObservationType());
        if (votCaboodleState == null && votEpicState == null) {
            RowState<VisitObservationType, VisitObservationTypeAudit> vot = getOrCreateObservationTypeState(msg.getInterfaceId(),
                    msg.getFlowsheetId(), msg.getSourceObservationType(), msg.getLastUpdatedInstant(), storedFrom);
            vot.saveEntityOrAuditLogIfRequired(visitObservationTypeRepo, visitObservationTypeAuditRepo);
        } else if (votCaboodleState != null) {
            VisitObservationType votCaboodle = votCaboodleState.getEntity();
            votCaboodleState.assignIfDifferent(msg.getInterfaceId(), votCaboodle.getInterfaceId(), votCaboodle::setInterfaceId);
            if (votEpicState != null) {
                replaceVisitObservationType(votEpicState.getEntity(), votCaboodle, msg.getLastUpdatedInstant(), storedFrom);
            }
            votCaboodleState.saveEntityOrAuditLogIfRequired(visitObservationTypeRepo, visitObservationTypeAuditRepo);
        } else { // state where votEpic exists and votCaboodle doesn't
            VisitObservationType votEpic = votEpicState.getEntity();
            votEpicState.assignIfDifferent(msg.getFlowsheetId(), votEpic.getIdInApplication(), votEpic::setIdInApplication);
            votEpicState.saveEntityOrAuditLogIfRequired(visitObservationTypeRepo, visitObservationTypeAuditRepo);
        }
    }

    /**
     * If two visit observation types had been created due to mapping information not being available at the time of creation,
     * once one of them is replaced, the linking in visit observation referring to the EPIC visit observation type need to be
     * replaced once information has been added to the caboodle visit observation type. After the linkage has been changed,
     * the superfluous EPIC visit observation type is deleted.
     * @param votEpic     Visit observation type generated through EPIC message, which needs to be deleted
     * @param votCaboodle Visit observation type that is enriched with mapping information and replaces EPIC visit observation type in
     *                    visit observations
     * @param validFrom   When information is valid from
     * @param storedFrom  When information was first processed in star
     */
    private void replaceVisitObservationType(VisitObservationType votEpic, VisitObservationType votCaboodle, Instant validFrom, Instant storedFrom) {
        for (VisitObservation visit : visitObservationRepo.findAllByVisitObservationTypeId(votEpic)) {
            RowState<VisitObservation, VisitObservationAudit> vState = new RowState<>(visit, validFrom,
                    storedFrom, false);
            vState.assignIfDifferent(votCaboodle, votEpic, visit::setVisitObservationTypeId);
            vState.saveEntityOrAuditLogIfRequired(visitObservationRepo, visitObservationAuditRepo);
        }
        deleteVisitObservationType(votEpic, validFrom, storedFrom);
    }

    /**
     * Finds existing visit observation type wrapped in row state or returns null.
     * @param interfaceId           Interface identifier of visit observation type to be retrieved.
     * @param idInApplication       Hospital flowsheet identifier of the visit observation type to be retrieved.
     * @param lastUpdated           Last updated date for visit observation type to be retrieved
     * @param storedFrom            When required visit observation type was stored from
     * @param sourceObservationType Source of required visit observation type
     * @return Required VisitObservationType wrapped in RowState or Null if VisitObservationType does not exist yet
     */
    private RowState<VisitObservationType, VisitObservationTypeAudit> getTypeStateOrNull(String interfaceId, String idInApplication,
                                                                                         Instant lastUpdated,
                                                                                         Instant storedFrom, String sourceObservationType) {
        return visitObservationTypeRepo
                .find(interfaceId, idInApplication, sourceObservationType)
                .map(vot -> new RowState<>(vot, lastUpdated, storedFrom, false))
                .orElse(null);
    }

    /**
     * Retrieves the existing information if visit observation type already exists, otherwise creates a new visit
     * observation type.
     * @param idInApplication Hospital flowsheet identifier
     * @param interfaceId     Interface id
     * @param observationType Type of visit observation
     * @param validFrom       When last updated
     * @param storedFrom      When this type of information was first processed from
     * @return RowState<VisitObservationType, VisitObservationTypeAudit> containing either existing or newly create repo information
     */
    private RowState<VisitObservationType, VisitObservationTypeAudit> getOrCreateObservationTypeState(
            String interfaceId, String idInApplication, String observationType, Instant validFrom, Instant storedFrom) {
        return visitObservationTypeRepo
                .find(interfaceId, idInApplication, observationType)
                .map(vot -> new RowState<>(vot, validFrom, storedFrom, false))
                .orElseGet(() -> createNewTypeInState(idInApplication, interfaceId, observationType, validFrom, storedFrom));
    }

    /**
     * Create a minimal visit observation type.
     * @param idInApplication       Id of the observation in the application
     * @param interfaceId           hl7 interface id
     * @param sourceObservationType type of visit observation
     * @param validFrom             Timestamp from which information valid from
     * @param storedFrom            time that emap-core started processing the message
     * @return minimal VisitObservationType wrapped in row state
     */
    private RowState<VisitObservationType, VisitObservationTypeAudit> createNewTypeInState(
            String idInApplication, String interfaceId, String sourceObservationType, Instant validFrom, Instant storedFrom) {
        VisitObservationType type = new VisitObservationType(idInApplication, interfaceId, sourceObservationType, validFrom, storedFrom);
        return new RowState<>(type, validFrom, storedFrom, true);
    }

    /**
     * Get or create existing observation entity.
     * @param msg             flowsheet
     * @param visit           hospital visit
     * @param observationType visit observation type
     * @param storedFrom      time that emap-core started processing the message
     * @return observation entity wrapped in RowState
     */
    private RowState<VisitObservation, VisitObservationAudit> getOrCreateFlowsheet(
            Flowsheet msg, HospitalVisit visit, VisitObservationType observationType, Instant storedFrom) {
        return visitObservationRepo
                .findByHospitalVisitIdAndVisitObservationTypeIdAndObservationDatetime(visit, observationType, msg.getObservationTime())
                .map(obs -> new RowState<>(obs, msg.getLastUpdatedInstant(), storedFrom, false))
                .orElseGet(() -> createMinimalFlowsheetState(msg, visit, observationType, storedFrom));
    }

    /**
     * Create minimal visit observation wrapped in RowState.
     * @param msg             flowsheet
     * @param visit           hospital visit
     * @param observationType visit observation type
     * @param storedFrom      time that emap-core started processing the message
     * @return minimal observation entity wrapped in RowState
     */
    private RowState<VisitObservation, VisitObservationAudit> createMinimalFlowsheetState(
            Flowsheet msg, HospitalVisit visit, VisitObservationType observationType, Instant storedFrom) {
        VisitObservation obs = new VisitObservation(visit, observationType, msg.getObservationTime(),
                msg.getSourceSystem(), msg.getLastUpdatedInstant(), storedFrom);
        return new RowState<>(obs, msg.getLastUpdatedInstant(), storedFrom, true);
    }

    /**
     * Update observation state from Flowsheet message.
     * @param msg              flowsheet
     * @param observationState observation entity wrapped in RowState
     * @throws RequiredDataMissingException if data type is not recognised for flowsheets
     */
    private void updateVisitObservation(Flowsheet msg, RowState<VisitObservation, VisitObservationAudit> observationState)
            throws RequiredDataMissingException {
        VisitObservation observation = observationState.getEntity();
        switch (msg.getValueType()) {
            case NUMERIC:
                observationState.assignInterchangeValue(msg.getNumericValue(), observation.getValueAsReal(), observation::setValueAsReal);
                break;
            case TEXT:
                observationState.assignInterchangeValue(msg.getStringValue(), observation.getValueAsText(), observation::setValueAsText);
                break;
            case DATE:
                observationState.assignInterchangeValue(msg.getDateValue(), observation.getValueAsDate(), observation::setValueAsDate);
                break;
            default:
                throw new RequiredDataMissingException(String.format("Flowsheet DataType '%s' not recognised", msg.getValueType()));
        }
        observationState.assignInterchangeValue(msg.getUnit(), observation.getUnit(), observation::setUnit);
        observationState.assignInterchangeValue(msg.getComment(), observation.getComment(), observation::setComment);
    }

    /**
     * Update the visit observation type from a flowsheet message.
     * @param msg             Flowsheet message
     * @param observationType Observation entity
     * @param validFrom       Time from which information valid from
     * @param storedFrom      Time that emap-core started processing the message
     */
    private void updateDataFlagsAndSaveObservationType(Flowsheet msg, VisitObservationType observationType, Instant validFrom, Instant storedFrom) {

        var rowState = new RowState<>(observationType, validFrom, storedFrom, false);
        rowState.assignIfDifferent(true, observationType.getHasVisitObservation(), observationType::setHasVisitObservation);

        // the isRealTime flag should only ever be set false -> true, not true -> false as there could be more live data
        var isRealTime = observationType.getIsRealTime();
        if (isRealTime == null || !isRealTime) {
            rowState.assignIfDifferent(msg.getIsRealTime(), isRealTime, observationType::setIsRealTime);
        }

        rowState.saveEntityOrAuditLogIfRequired(visitObservationTypeRepo, visitObservationTypeAuditRepo);
    }
}


/**
 * Helper component, used because Spring cache doesn't intercept self-invoked method calls.
 */
@Component
class VisitObservationCache {

    private final VisitObservationTypeRepository visitObservationTypeRepo;

    VisitObservationCache(VisitObservationTypeRepository visitObservationTypeRepo) {
        this.visitObservationTypeRepo = visitObservationTypeRepo;
    }

    /**
     * Get or create visit observation type, persisting and caching the output of this method.
     * @param idInApplication Id of the observation in the application
     * @param interfaceId     Id of observation type in HL7 messages
     * @param observationType type of observation (e.g. flowsheet)
     * @param validFrom       Timestamp from which information valid from
     * @param storedFrom      time that emap-core started processing the message
     * @return persisted VisitObservationType
     */
    @Cacheable(value = "visitObservationType", key = "{ #interfaceId, #idInApplication, #observationType }")
    public VisitObservationType getOrCreatePersistedObservationType(
            String interfaceId, String idInApplication, String observationType, Instant validFrom, Instant storedFrom) {
        return visitObservationTypeRepo
                .find(interfaceId, idInApplication, observationType)
                .orElseGet(() -> {
                    VisitObservationType type = new VisitObservationType(idInApplication, interfaceId, observationType, validFrom, storedFrom);
                    return visitObservationTypeRepo.save(type);
                });
    }
}
