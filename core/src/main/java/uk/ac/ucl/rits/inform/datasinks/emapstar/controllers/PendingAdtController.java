package uk.ac.ucl.rits.inform.datasinks.emapstar.controllers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import uk.ac.ucl.rits.inform.datasinks.emapstar.RowState;
import uk.ac.ucl.rits.inform.datasinks.emapstar.repos.PlannedMovementAuditRepository;
import uk.ac.ucl.rits.inform.datasinks.emapstar.repos.PlannedMovementRepository;
import uk.ac.ucl.rits.inform.informdb.identity.HospitalVisit;
import uk.ac.ucl.rits.inform.informdb.movement.Location;
import uk.ac.ucl.rits.inform.informdb.movement.PlannedMovement;
import uk.ac.ucl.rits.inform.informdb.movement.PlannedMovementAudit;
import uk.ac.ucl.rits.inform.interchange.adt.AdmitPatient;
import uk.ac.ucl.rits.inform.interchange.adt.AdtCancellation;
import uk.ac.ucl.rits.inform.interchange.adt.AdtMessage;
import uk.ac.ucl.rits.inform.interchange.adt.CancelPendingTransfer;
import uk.ac.ucl.rits.inform.interchange.adt.HospitalService;
import uk.ac.ucl.rits.inform.interchange.adt.PendingTransfer;
import uk.ac.ucl.rits.inform.interchange.adt.CancelPendingDischarge;
import uk.ac.ucl.rits.inform.interchange.adt.PendingDischarge;

import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;



/**
 * Carries out the business logic part of pending ADT messages.
 * This could be added to the patient location controller, but it seemed big enough already.
 * When we have ADT edits (ADT^Z99) set up, and potentially the individual movement IDs in hl7 messages,
 * it may be worth thinking about the structure - hopefully less out-of-order processing would be required so the location visit should be simpler.
 * @author Stef Piatek
 */
@Component
public class PendingAdtController {
    private final Logger logger = LoggerFactory.getLogger(PendingAdtController.class);
    private LocationController locationController;
    private final PlannedMovementRepository plannedMovementRepo;
    private final PlannedMovementAuditRepository plannedMovementAuditRepo;

    /**
     * Using a FindMovement interface so that we can reuse the same get or create method, using a different repository method.
     */
    private final FindMovement allFromRequest;
    private final FindMovement allFromCancel;

    /**
     * @param locationController       To get the location entity for the planned move
     * @param plannedMovementRepo      To update the Planned Movement entity
     * @param plannedMovementAuditRepo To update the Audit log for Planned Movement
     */
    public PendingAdtController(LocationController locationController,
                                PlannedMovementRepository plannedMovementRepo,
                                PlannedMovementAuditRepository plannedMovementAuditRepo) {
        this.locationController = locationController;
        this.plannedMovementRepo = plannedMovementRepo;
        this.plannedMovementAuditRepo = plannedMovementAuditRepo;
        allFromRequest = plannedMovementRepo::findMatchingMovementsFromRequest;
        allFromCancel = plannedMovementRepo::findMatchingMovementsFromCancel;
    }


    /**
     * Process pending ADT request.
     * <p>
     * The Hl7 feed will eventually be changed so that we have an identifier per pending transfer, until then we guarantee the order of cancellations.
     * If we get messages out of order and have several cancellation messages before we receive any requests,
     * then the first request message for the location and encounter will add the eventDatetime to the earliest cancellation.
     * Subsequent requests will add the eventDatetime to the earliest cancellation with no eventDatetime, or create a new request if none exist
     * after the pending request eventDatetime.
     * @param visit      associated visit
     * @param msg        pending adt
     * @param validFrom  time in the hospital when the message was created
     * @param storedFrom time that emap core started processing the message
     */
    public void processMsg(HospitalVisit visit, PendingTransfer msg, Instant validFrom, Instant storedFrom) {
        Location plannedLocation = null;
        if (msg.getPendingDestination().isSave()) {
            plannedLocation = locationController.getOrCreateLocation(msg.getPendingDestination().get());
        }

        RowState<PlannedMovement, PlannedMovementAudit> plannedState = getOrCreate(
                allFromRequest, visit, plannedLocation, msg.getPendingEventType().toString(), msg.getEventOccurredDateTime(), validFrom, storedFrom
        );
        addHospitalService(msg, plannedState);

        PlannedMovement plannedMovement = plannedState.getEntity();
        // If we receive a cancelled message before the original request then add it in
        if (plannedMovement.getEventDatetime() == null) {
            plannedState.assignIfDifferent(msg.getEventOccurredDateTime(), plannedMovement.getEventDatetime(), plannedMovement::setEventDatetime);
        }

        plannedState.saveEntityOrAuditLogIfRequired(plannedMovementRepo, plannedMovementAuditRepo);
    }
    /**
     * Process pending ADT request (discharge).
     * <p>
     * The Hl7 feed will eventually be changed so that we have an identifier per pending transfer, until then we guarantee the order of cancellations.
     * If we get messages out of order and have several cancellation messages before we receive any requests,
     * then the first request message for the location and encounter will add the eventDatetime to the earliest cancellation.
     * Subsequent requests will add the eventDatetime to the earliest cancellation with no eventDatetime, or create a new request if none exist
     * after the pending request eventDatetime.
     * @param visit      associated visit
     * @param msg        pending adt
     * @param validFrom  time in the hospital when the message was created
     * @param storedFrom time that emap core started processing the message
     */
    public void processMsg(HospitalVisit visit, PendingDischarge msg, Instant validFrom, Instant storedFrom) {
        Location plannedLocation = null;
        if (msg.getPendingDestination().isSave()) {
            plannedLocation = locationController.getOrCreateLocation(msg.getPendingDestination().get());
        }

        RowState<PlannedMovement, PlannedMovementAudit> plannedState = getOrCreate(
                allFromRequest, visit, plannedLocation, msg.getPendingEventType().toString(), msg.getEventOccurredDateTime(), validFrom, storedFrom
        );

        PlannedMovement plannedMovement = plannedState.getEntity();
        // If we receive a cancelled message before the original request then add it in
        if (plannedMovement.getEventDatetime() == null) {
            plannedState.assignIfDifferent(msg.getEventOccurredDateTime(), plannedMovement.getEventDatetime(), plannedMovement::setEventDatetime);
        }

        plannedState.saveEntityOrAuditLogIfRequired(plannedMovementRepo, plannedMovementAuditRepo);
    }
    /**
     * Get existing planned movement or create a new one, with parameterised query.
     * @param findMovement     method reference for how to get an optional PlannedMovement from the database
     * @param visit            associated visit (used in query)
     * @param plannedLocation  destination for the planned movement (can be null, used in query)
     * @param pendingEventType type of pending event (used in query)
     * @param eventDateTime    time that the pending event occurred (used in query)
     * @param validFrom        time in the hospital when the message was created
     * @param storedFrom       time that emap core started processing the message
     * @return row state of planned movement
     */
    private RowState<PlannedMovement, PlannedMovementAudit> getOrCreate(
            FindMovement findMovement, HospitalVisit visit, Location plannedLocation, String pendingEventType, Instant eventDateTime,
            Instant validFrom, Instant storedFrom) {
        logger.debug("Getting or creating PendingMovement");
        return findFirstMovement(visit, plannedLocation, pendingEventType, eventDateTime, findMovement)
                .map(pm -> new RowState<>(pm, validFrom, storedFrom, false))
                .orElseGet(() -> new RowState<>(new PlannedMovement(visit, plannedLocation, pendingEventType), validFrom, storedFrom, true));
    }

    private Optional<PlannedMovement> findFirstMovement(
            HospitalVisit visit, Location plannedLocation, String pendingType, Instant eventDateTime, FindMovement findMovement) {
        List<PlannedMovement> movements = findMovement.matching(pendingType, visit, plannedLocation, eventDateTime);
        return movements.stream().findFirst();
    }

    /**
     * Process pending ADT cancellation.
     * <p>
     * If multiple pending ADT events exist that aren't cancelled, will cancel the earliest one that occurs before the cancellation time.
     * @param visit      associated visit
     * @param msg        pending adt cancellation msg
     * @param validFrom  time in the hospital when the message was created
     * @param storedFrom time that emap core started processing the message
     */
    public void processMsg(HospitalVisit visit, CancelPendingTransfer msg, Instant validFrom, Instant storedFrom) {
        Location plannedLocation = null;
        if (msg.getPendingDestination().isSave()) {
            plannedLocation = locationController.getOrCreateLocation(msg.getPendingDestination().get());
        }

        RowState<PlannedMovement, PlannedMovementAudit> plannedState = getOrCreate(
                allFromCancel, visit, plannedLocation, msg.getPendingEventType().toString(), msg.getCancelledDateTime(), validFrom, storedFrom
        );
        PlannedMovement plannedMovement = plannedState.getEntity();
        // Cancel the message if it hasn't been cancelled already
        if (plannedMovement.getCancelledDatetime() == null) {
            plannedState.assignIfDifferent(msg.getCancelledDateTime(), plannedMovement.getCancelledDatetime(), plannedMovement::setCancelledDatetime);
            plannedState.assignIfDifferent(true, plannedMovement.getCancelled(), plannedMovement::setCancelled);
        }

        plannedState.saveEntityOrAuditLogIfRequired(plannedMovementRepo, plannedMovementAuditRepo);
    }

     /**
     * Process a real ADT-triggered change of hospital service/subspeciality, as a fallback for when
     * a matching pending transfer request was never sent (e.g. Z99 edits, or a real admission/update
     * that carries a hospital service different to the one currently recorded).
     * <p>
     * The Hl7 feed will eventually be changed so that we have an identifier per pending transfer, until then we guarantee the order of cancellations.
     * If we get messages out of order and have several cancellation messages before we receive any requests,
     * then the first request message for the location and encounter will add the eventDatetime to the earliest cancellation.
     * Subsequent requests will add the eventDatetime to the earliest cancellation with no eventDatetime, or create a new request if none exist
     * after the pending request eventDatetime.
     * @param visit      associated visit
     * @param msg        the ADT message, for the fields shared by all ADT messages
     * @param serviceMsg the same message, as its HospitalService view
     * @param validFrom  time in the hospital when the message was created
     * @param storedFrom time that emap core started processing the message
     */
    public void processHospitalServiceFallback(HospitalVisit visit, AdtMessage msg, HospitalService serviceMsg,
                                                 Instant validFrom, Instant storedFrom) {
        if (serviceMsg.getHospitalService().isUnknown()) {
            return;
        }

        Location fullLocation = null;
        if (msg.getFullLocationString().isSave()) {
            fullLocation = locationController.getOrCreateLocation(msg.getFullLocationString().get());
        }

        Instant eventDateTime = msg.getEventOccurredDateTime();

        List<PlannedMovement> movements = plannedMovementRepo.findMatchingMovementsForHospitalServiceFallback(visit, fullLocation, eventDateTime);
        if (!movements.isEmpty()) {

            int mostRecentMoveIndex = movements.size() - 1;
            String currentService = movements.get(mostRecentMoveIndex).getHospitalService();
            String editedService = serviceMsg.getHospitalService().get();

            if (!Objects.equals(currentService, editedService)) {
                Long matchedMovementId = movements.get(mostRecentMoveIndex).getPlannedMovementId();
                RowState<PlannedMovement, PlannedMovementAudit> plannedState = getOrCreate(
                        allFromRequest, visit, fullLocation, "EDIT/HOSPITAL_SERVICE_CHANGE", eventDateTime, validFrom, storedFrom
                );
                PlannedMovement movement = plannedState.getEntity();
                // not sure why but event date time isn't being set. Add it here.
                plannedState.assignIfDifferent(eventDateTime, movement.getEventDatetime(), movement::setEventDatetime);
                plannedState.assignInterchangeValue(serviceMsg.getHospitalService(), movement.getHospitalService(), movement::setHospitalService);
                plannedState.assignIfDifferent(matchedMovementId, movement.getMatchedMovementId(), movement::setMatchedMovementId);
                plannedState.saveEntityOrAuditLogIfRequired(plannedMovementRepo, plannedMovementAuditRepo);
            }
        }
    }

    /**
     * Record a real admission as its own planned movement.
     * <p>
     * Unlike {@link #processHospitalServiceFallback}, this always inserts (or idempotently reuses) an
     * ADMISSION row, regardless of whether a matching planned movement already exists or has the same
     * hospital service. Any matched prior movement (e.g. a TRANSFER row from a pending transfer request)
     * is only referenced via matchedMovementId - it is never itself modified.
     * @param visit      associated visit
     * @param msg        the admission message
     * @param validFrom  time in the hospital when the message was created
     * @param storedFrom time that emap core started processing the message
     */
    public void processAdmission(HospitalVisit visit, AdmitPatient msg, Instant validFrom, Instant storedFrom) {
        if (msg.getHospitalService().isUnknown()) {
            return;
        }

        Location fullLocation = null;
        if (msg.getFullLocationString().isSave()) {
            fullLocation = locationController.getOrCreateLocation(msg.getFullLocationString().get());
        }
        Instant eventDateTime = msg.getEventOccurredDateTime();

        List<PlannedMovement> priorMovements = plannedMovementRepo.findMatchingMovementsForHospitalServiceFallback(
                visit, fullLocation, eventDateTime);
        Long matchedMovementId = priorMovements.isEmpty() ? null : priorMovements.get(priorMovements.size() - 1).getPlannedMovementId();

        RowState<PlannedMovement, PlannedMovementAudit> plannedState = getOrCreate(
                allFromRequest, visit, fullLocation, "ADMISSION", eventDateTime, validFrom, storedFrom
        );
        PlannedMovement movement = plannedState.getEntity();
        plannedState.assignIfDifferent(eventDateTime, movement.getEventDatetime(), movement::setEventDatetime);
        plannedState.assignInterchangeValue(msg.getHospitalService(), movement.getHospitalService(), movement::setHospitalService);
        if (matchedMovementId != null) {
            plannedState.assignIfDifferent(matchedMovementId, movement.getMatchedMovementId(), movement::setMatchedMovementId);
        }
        plannedState.saveEntityOrAuditLogIfRequired(plannedMovementRepo, plannedMovementAuditRepo);
    }

    /**
     * Process pending ADT cancellation.
     * <p>
     * If multiple pending ADT events exist that aren't cancelled, will cancel the earliest one that occurs before the cancellation time.
     * @param visit      associated visit
     * @param msg        pending adt cancellation msg
     * @param validFrom  time in the hospital when the message was created
     * @param storedFrom time that emap core started processing the message
     */
    public void processMsg(HospitalVisit visit, CancelPendingDischarge msg, Instant validFrom, Instant storedFrom) {
        Location plannedLocation = null;
        if (msg.getPendingDestination().isSave()) {
            plannedLocation = locationController.getOrCreateLocation(msg.getPendingDestination().get());
        }

        RowState<PlannedMovement, PlannedMovementAudit> plannedState = getOrCreate(
                allFromCancel, visit, plannedLocation, msg.getPendingEventType().toString(), msg.getCancelledDateTime(), validFrom, storedFrom
        );
        PlannedMovement plannedMovement = plannedState.getEntity();
        // Cancel the message if it hasn't been cancelled already
        if (plannedMovement.getCancelledDatetime() == null) {
            plannedState.assignIfDifferent(msg.getCancelledDateTime(), plannedMovement.getCancelledDatetime(), plannedMovement::setCancelledDatetime);
            plannedState.assignIfDifferent(true, plannedMovement.getCancelled(), plannedMovement::setCancelled);
        }

        plannedState.saveEntityOrAuditLogIfRequired(plannedMovementRepo, plannedMovementAuditRepo);
    }


    /**
     * Cancel the matching ADMISSION row for a cancelled admission.
     * <p>
     * Only rows created by {@link #processAdmission} (event type ADMISSION) are ever cancelled here.
     * A row originating from a real pending transfer/discharge request is left alone, since it
     * represents a separately-tracked plan that may still be valid regardless of this one ADT message.
     * @param visit        associated visit
     * @param msg          the ADT message, for the fields shared by all ADT messages
     * @param cancellation the same message, as its AdtCancellation view
     * @param validFrom    time in the hospital when the message was created
     * @param storedFrom   time that emap core started processing the message
     */
    public void processAdmissionCancellation(HospitalVisit visit, AdtMessage msg, AdtCancellation cancellation,
                                              Instant validFrom, Instant storedFrom) {
        Location fullLocation = null;
        if (msg.getFullLocationString().isSave()) {
            fullLocation = locationController.getOrCreateLocation(msg.getFullLocationString().get());
        }

        List<PlannedMovement> movements = plannedMovementRepo.findMatchingMovementsForHospitalServiceFallback(
                visit, fullLocation, cancellation.getCancelledDateTime());
        if (movements.isEmpty()) {
            return;
        }

        PlannedMovement movement = movements.get(movements.size() - 1);
        if (!"ADMISSION".equals(movement.getEventType()) || movement.getCancelledDatetime() != null) {
            return;
        }

        RowState<PlannedMovement, PlannedMovementAudit> plannedState = new RowState<>(movement, validFrom, storedFrom, false);
        plannedState.assignIfDifferent(cancellation.getCancelledDateTime(), movement.getCancelledDatetime(), movement::setCancelledDatetime);
        plannedState.assignIfDifferent(true, movement.getCancelled(), movement::setCancelled);
        plannedState.saveEntityOrAuditLogIfRequired(plannedMovementRepo, plannedMovementAuditRepo);
    }

    /**
     * Delete planned movements from a delete patient information message.
     * @param visit            Hospital visit that should have their planned movements deleted
     * @param invalidationTime Time of the delete information message
     * @param deletionTime     time that emap-core started processing the message.
     */
    public void deletePlannedMovements(HospitalVisit visit, Instant invalidationTime, Instant deletionTime) {
        plannedMovementRepo.findAllByHospitalVisitId(visit)
                .forEach(plannedMovement -> deletePlannedMovement(plannedMovement, invalidationTime, deletionTime));
    }

    /**
     * Audit and delete a planned movement.
     * @param plannedMovement Planned movement to delete
     * @param deletionTime    Hospital time that the planned movement was deleted at
     * @param storedUntil     Time that emap-core started processing the message.
     */
    private void deletePlannedMovement(PlannedMovement plannedMovement, Instant deletionTime, Instant storedUntil) {
        plannedMovementAuditRepo.save(plannedMovement.createAuditEntity(deletionTime, storedUntil));
        plannedMovementRepo.delete(plannedMovement);
    }

    /**
     * Add hospital service.
     * @param msg           PendingTransfer
     * @param movementState movement wrapped in state class
     */
    private void addHospitalService(final PendingTransfer msg, RowState<PlannedMovement, PlannedMovementAudit> movementState) {
        PlannedMovement movement = movementState.getEntity();
        movementState.assignInterchangeValue(msg.getHospitalService(), movement.getHospitalService(), movement::setHospitalService);
    }

}

/**
 * Interface to allow passing of the repository method as a parameter.
 * <p>
 * This single method interface is effectively implemented by the find methods in the PlannedMovementRepository
 * @author Stef Piatek
 */
interface FindMovement {
    List<PlannedMovement> matching(String eventType, HospitalVisit hospitalVisitId, Location plannedLocation, Instant eventDatetime);
}
