package uk.ac.ucl.rits.inform.datasinks.emapstar.controllers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import uk.ac.ucl.rits.inform.datasinks.emapstar.RowState;
import uk.ac.ucl.rits.inform.datasinks.emapstar.repos.ConsultationRequestAuditRepository;
import uk.ac.ucl.rits.inform.datasinks.emapstar.repos.ConsultationRequestRepository;
import uk.ac.ucl.rits.inform.datasinks.emapstar.repos.ConsultationTypeAuditRepository;
import uk.ac.ucl.rits.inform.datasinks.emapstar.repos.ConsultationTypeRepository;
import uk.ac.ucl.rits.inform.informdb.consults.ConsultationRequest;
import uk.ac.ucl.rits.inform.informdb.consults.ConsultationRequestAudit;
import uk.ac.ucl.rits.inform.informdb.consults.ConsultationType;
import uk.ac.ucl.rits.inform.informdb.consults.ConsultationTypeAudit;
import uk.ac.ucl.rits.inform.informdb.identity.HospitalVisit;
import uk.ac.ucl.rits.inform.interchange.ConsultMetadata;
import uk.ac.ucl.rits.inform.interchange.ConsultRequest;

import javax.annotation.Resource;
import java.time.Instant;
import java.util.List;

/**
 * Functionality to create consultation requests for patients.
 * <p>
 * A consultation request is initiated when specialist advice on a patient's condition is requested.
 * @author Anika Cawthorn
 * @author Stef Piatek
 */
@Component
public class ConsultationRequestController {
    @Resource
    private ConsultCache cache;
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final ConsultationRequestRepository consultationRequestRepo;
    private final ConsultationTypeRepository consultationTypeRepo;
    private final ConsultationTypeAuditRepository consultationTypeAuditRepo;
    private final ConsultationRequestAuditRepository consultationRequestAuditRepo;
    private final QuestionController questionController;

    /**
     * Setting repositories holding information on consultation requests.
     * @param consultationRequestRepo      Consultation request repo
     * @param consultationTypeRepo         Consultation request type repo
     * @param consultationTypeAuditRepo    Audit for Consultation requests
     * @param consultationRequestAuditRepo Consultation request audit type repo
     * @param questionController           Question controller for questions in relation to consultation requests
     */
    public ConsultationRequestController(
            ConsultationRequestRepository consultationRequestRepo, ConsultationRequestAuditRepository consultationRequestAuditRepo,
            ConsultationTypeRepository consultationTypeRepo, ConsultationTypeAuditRepository consultationTypeAuditRepo,
            QuestionController questionController) {
        this.consultationRequestRepo = consultationRequestRepo;
        this.consultationTypeRepo = consultationTypeRepo;
        this.consultationTypeAuditRepo = consultationTypeAuditRepo;
        this.consultationRequestAuditRepo = consultationRequestAuditRepo;
        this.questionController = questionController;
    }

    /**
     * Update or create consultation request metadata.
     * <p>
     * Also evicts cache for existing metadata
     * @param msg        consultation metadata message
     * @param storedFrom time that the message was started to be processed by star
     */
    @Transactional
    @CacheEvict(value = "consultationType", key = "#msg.code")
    public void processMessage(final ConsultMetadata msg, final Instant storedFrom) {
        RowState<ConsultationType, ConsultationTypeAudit> consultationState = getOrCreateTypeState(
                msg.getCode(), msg.getLastUpdatedDate(), storedFrom);
        ConsultationType consultationType = consultationState.getEntity();

        if (consultationTypeShouldBeUpdated(msg.getLastUpdatedDate(), consultationType)) {
            consultationState.assignIfDifferent(msg.getName(), consultationType.getName(), consultationType::setName);
        }
        consultationState.saveEntityOrAuditLogIfRequired(consultationTypeRepo, consultationTypeAuditRepo);
    }

    /**
     * Consultation type should be updated if it has no name data, or if the current message is newer.
     * @param messageTime      time that the message is valid from
     * @param consultationType consultation type entity
     * @return true if the consultation type should be updated
     */
    private boolean consultationTypeShouldBeUpdated(Instant messageTime, ConsultationType consultationType) {
        return consultationType.getName() == null || messageTime.isAfter(consultationType.getValidFrom());
    }

    /**
     * Process consultation request message.
     * @param msg        Consultation request message
     * @param visit      Hospital visit this consultation request relates to.
     * @param storedFrom time that the message was started to be processed by star
     */
    @Transactional
    public void processMessage(final ConsultRequest msg, HospitalVisit visit, final Instant storedFrom) {
        ConsultationType consultType = cache.getOrCreateMinimalType(msg.getConsultationType(), msg.getStatusChangeDatetime(), storedFrom);
        RowState<ConsultationRequest, ConsultationRequestAudit> consultationRequest = getOrCreateConsultationRequest(
                msg, visit, consultType, storedFrom);

        if (consultRequestShouldBeUpdated(msg, consultationRequest)) {
            updateConsultRequest(msg, consultationRequest);
        }

        consultationRequest.saveEntityOrAuditLogIfRequired(consultationRequestRepo, consultationRequestAuditRepo);
        questionController.processQuestions(msg.getQuestions(), ParentTableType.CONSULT_REQUEST.toString(),
                consultationRequest.getEntity().getConsultationRequestId(), msg.getStatusChangeDatetime(), storedFrom);
    }

    /**
     * Create or create consultation type state.
     * @param code            Consultation type code
     * @param messageDatetime Time that the message data was last updated
     * @param storedFrom      When star started processing this message
     * @return ConsultationType wrapped in row state
     */
    private RowState<ConsultationType, ConsultationTypeAudit> getOrCreateTypeState(
            String code, Instant messageDatetime, Instant storedFrom) {
        return consultationTypeRepo
                .findByCode(code)
                .map(msg -> new RowState<>(msg, messageDatetime, storedFrom, false))
                .orElseGet(() -> createNewType(code, messageDatetime, storedFrom));
    }

    /**
     * Create a minimal ConsultationType.
     * @param code            Consultation type code
     * @param messageDatetime Time that the message data was last updated
     * @param storedFrom      Time that emap-core started processing the message
     * @return ConsultationType wrapped in row state
     */
    private RowState<ConsultationType, ConsultationTypeAudit> createNewType(String code, Instant messageDatetime, Instant storedFrom) {
        ConsultationType consultationType = new ConsultationType(code, messageDatetime, storedFrom);
        logger.debug("Created new {}", consultationType);
        return new RowState<>(consultationType, messageDatetime, storedFrom, true);
    }

    /**
     * Get or create existing ConsultationRequest entity.
     * @param msg              Consultation request message
     * @param visit            Hospital visit of patient this consultation request refers to.
     * @param consultationType Consultancy type as identified in message
     * @param storedFrom       Time that emap-core started processing the message
     * @return ConsultationRequest entity wrapped in RowState
     */
    private RowState<ConsultationRequest, ConsultationRequestAudit> getOrCreateConsultationRequest(
            ConsultRequest msg, HospitalVisit visit, ConsultationType consultationType, Instant storedFrom) {
        return consultationRequestRepo
                .findByInternalId(msg.getEpicConsultId())
                .map(obs -> new RowState<>(obs, msg.getStatusChangeDatetime(), storedFrom, false))
                .orElseGet(() -> createMinimalConsultationRequest(msg, visit, consultationType, storedFrom));
    }

    /**
     * Create minimal consultation request wrapped in RowState.
     * @param msg              Consultation request message
     * @param visit            Hospital visit of the patient consultation request occurred at
     * @param consultationType Consultation request type referred to in message
     * @param storedFrom       Time that emap-core started processing the message
     * @return minimal consultation request wrapped in RowState
     */
    private RowState<ConsultationRequest, ConsultationRequestAudit> createMinimalConsultationRequest(
            ConsultRequest msg, HospitalVisit visit, ConsultationType consultationType,
            Instant storedFrom) {
        ConsultationRequest consultationRequest = new ConsultationRequest(consultationType, visit,
                msg.getEpicConsultId());
        logger.debug("Created new {}", consultationRequest);
        return new RowState<>(consultationRequest, msg.getStatusChangeDatetime(), storedFrom, true);
    }

    /**
     * Should the consult request be updated (for fields that can change over time).
     * @param msg                 Consultation request message
     * @param consultationRequest Consultation request
     * @return true if message should be updated
     */
    private boolean consultRequestShouldBeUpdated(ConsultRequest msg, RowState<ConsultationRequest,
            ConsultationRequestAudit> consultationRequest) {
        return (consultationRequest.isEntityCreated() || !msg.getStatusChangeDatetime().isBefore(
                consultationRequest.getEntity().getValidFrom()));
    }

    /**
     * Update consultation request from consultation request message.
     * @param msg          consultation request message
     * @param requestState consultation request referred to in message
     */
    private void updateConsultRequest(ConsultRequest msg, RowState<ConsultationRequest,
            ConsultationRequestAudit> requestState) {
        ConsultationRequest request = requestState.getEntity();

        requestState.assignIfDifferent(msg.getScheduledDatetime(), request.getScheduledDatetime(), request::setScheduledDatetime);
        requestState.assignInterchangeValue(msg.getNotes(), request.getComments(), request::setComments);
        requestState.assignIfDifferent(msg.isCancelled(), request.getCancelled(), request::setCancelled);
        requestState.assignIfDifferent(msg.isClosedDueToDischarge(), request.getClosedDueToDischarge(), request::setClosedDueToDischarge);
        // only update status change time if the entity has been created or updated
        if (requestState.isEntityCreated() || requestState.isEntityUpdated()) {
            requestState.assignIfDifferent(msg.getStatusChangeDatetime(), request.getStatusChangeDatetime(), request::setStatusChangeDatetime);
        }
    }

    /**
     * Deletes consult requests that are older than the current message.
     * @param visit             Hospital Visit Entity
     * @param invalidationTime  Lab Battery
     * @param deletionTime      Lab Sample entity
     */
    public void deleteConsultRequestsForVisit(HospitalVisit visit, Instant invalidationTime, Instant deletionTime) {
        List<ConsultationRequest> consultationRequests = consultationRequestRepo.findAllByHospitalVisitId(visit);
        for (var cr : consultationRequests) {
            ConsultationRequestAudit auditEntity = cr.createAuditEntity(invalidationTime, deletionTime);
            consultationRequestAuditRepo.save(auditEntity);
            consultationRequestRepo.delete(cr);
        }
    }
}

/**
 * Helper component, used because Spring cache doesn't intercept self-invoked method calls.
 */
@Component
class ConsultCache {
    private final ConsultationTypeRepository consultationTypeRepo;

    /**
     * @param consultationTypeRepo Consultation request type repo
     */
    ConsultCache(ConsultationTypeRepository consultationTypeRepo) {
        this.consultationTypeRepo = consultationTypeRepo;
    }


    /**
     * Get existing consult type or create a new saved minimal consult type if it doesn't exist.
     * <p>
     * Will cache the consultation type returned from this method.
     * @param code            Consultation type code
     * @param messageDatetime Time that the message data was last updated
     * @param storedFrom      When star started processing this message
     * @return consult type
     */
    @Cacheable(value = "consultationType", key = "#code")
    public ConsultationType getOrCreateMinimalType(String code, Instant messageDatetime, Instant storedFrom) {
        return consultationTypeRepo
                .findByCode(code)
                .orElseGet(() -> {
                    ConsultationType type = new ConsultationType(code, messageDatetime, storedFrom);
                    return consultationTypeRepo.save(type);
                });
    }

}
