package uk.ac.ucl.rits.inform.datasinks.emapstar.controllers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import uk.ac.ucl.rits.inform.datasinks.emapstar.RowState;
import uk.ac.ucl.rits.inform.datasinks.emapstar.exceptions.RequiredDataMissingException;
import uk.ac.ucl.rits.inform.datasinks.emapstar.repos.vist_observations.VisitObservationAuditRepository;
import uk.ac.ucl.rits.inform.datasinks.emapstar.repos.vist_observations.VisitObservationRepository;
import uk.ac.ucl.rits.inform.datasinks.emapstar.repos.vist_observations.VisitObservationTypeAuditRepository;
import uk.ac.ucl.rits.inform.datasinks.emapstar.repos.vist_observations.VisitObservationTypeRepository;
import uk.ac.ucl.rits.inform.informdb.identity.HospitalVisit;
import uk.ac.ucl.rits.inform.informdb.visit_recordings.VisitObservation;
import uk.ac.ucl.rits.inform.informdb.visit_recordings.VisitObservationAudit;
import uk.ac.ucl.rits.inform.informdb.visit_recordings.VisitObservationType;
import uk.ac.ucl.rits.inform.informdb.visit_recordings.VisitObservationTypeAudit;
import uk.ac.ucl.rits.inform.interchange.Flowsheet;
import uk.ac.ucl.rits.inform.interchange.FlowsheetMetadata;

import java.time.Instant;

/**
 * Interactions with observation visits.
 * @author Stef Piatek
 */
@Component
public class VisitObservationController {
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
     * Create, update or delete a flowsheet.
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

        RowState<VisitObservationType, VisitObservationTypeAudit> typeState = getOrCreateObservationType(msg, storedFrom);
        typeState.saveEntityOrAuditLogIfRequired(visitObservationTypeRepo, visitObservationTypeAuditRepo);

        RowState<VisitObservation, VisitObservationAudit> flowsheetState = getOrCreateFlowsheet(msg, visit, typeState.getEntity(), storedFrom);
        if (messageShouldBeUpdated(msg, flowsheetState)) {
            updateVisitObservation(msg, flowsheetState);
            flowsheetState.saveEntityOrAuditLogIfRequired(visitObservationRepo, visitObservationAuditRepo);
        }
    }

    /**
     * Get existing observation type or create and save minimal observation type.
     * @param msg flowsheet
     * @return VisitObservationType
     */
    private RowState<VisitObservationType, VisitObservationTypeAudit> getOrCreateObservationType(Flowsheet msg, Instant storedFrom) {
        return visitObservationTypeRepo
                .findByIdInApplicationAndSourceSystemAndSourceApplication(msg.getFlowsheetId(), msg.getSourceSystem(), msg.getSourceApplication())
                .map(vot -> new RowState<>(vot, msg.getUpdatedTime(), storedFrom, false))
                .orElseGet(() -> createNewType(msg, storedFrom));
    }

    /**
     * Create and save a minimal visit observation type.
     * @param msg flowsheet
     * @return saved minimal VisitObservationType
     */
    private RowState<VisitObservationType, VisitObservationTypeAudit> createNewType(Flowsheet msg, Instant storedFrom) {
        VisitObservationType type = new VisitObservationType(msg.getFlowsheetId(), msg.getSourceSystem(), msg.getSourceApplication());
        return new RowState<>(type, msg.getUpdatedTime(), storedFrom, true);
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
                .map(obs -> new RowState<>(obs, msg.getUpdatedTime(), storedFrom, false))
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
        VisitObservation obs = new VisitObservation(visit, observationType, msg.getObservationTime(), msg.getUpdatedTime(), storedFrom);
        return new RowState<>(obs, msg.getUpdatedTime(), storedFrom, true);
    }

    /**
     * Update message if observation has been created, or the message updated time is >= entity validFrom.
     * @param msg              flowsheet
     * @param observationState observation entity wrapped in RowState
     * @return true if message should be updated
     */
    private boolean messageShouldBeUpdated(Flowsheet msg, RowState<VisitObservation, VisitObservationAudit> observationState) {
        return observationState.isEntityCreated() || !msg.getUpdatedTime().isBefore(observationState.getEntity().getValidFrom());
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

}

