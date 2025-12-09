package uk.ac.ucl.rits.inform.datasinks.emapstar.repos;

import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.CrudRepository;
import uk.ac.ucl.rits.inform.informdb.identity.HospitalVisit;
import uk.ac.ucl.rits.inform.informdb.movement.Location;
import uk.ac.ucl.rits.inform.informdb.movement.PlannedMovement;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

/**
 * Interaction with the PlannedMovement table.
 * @author Stef Piatek
 */
public interface PlannedMovementRepository extends CrudRepository<PlannedMovement, Long> {
    /**
     * Try and find a matching planned movement from a pending adt request message.
     * <p>
     * Always find by the event type, planned location and hospital visit Id, then:
     * - For in order messages: message which has the same event date time
     * - For cancel message arriving before the original pending adt: find message which has a null eventDatetime and a cancel after current datetime
     * @param eventType       type of the planned movement
     * @param hospitalVisitId hospital visit associated with the movement
     * @param plannedLocation planned location for the movement
     * @param eventDatetime   the datetime that event was created
     * @return planned movement entities
     */
    @Query("from PlannedMovement "
            + "where eventType = :eventType and hospitalVisitId = :hospitalVisitId "
            + "and (locationId = :plannedLocation or (:plannedLocation is null and locationId is null)) "
            + "and ((eventDatetime = :eventDatetime) "
            + "      or (cancelledDatetime >= :eventDatetime and eventDatetime is null) "
            + "     )"
            + "order by eventDatetime, cancelledDatetime "
    )
    List<PlannedMovement> findMatchingMovementsFromRequest(
            String eventType, HospitalVisit hospitalVisitId, Location plannedLocation, Instant eventDatetime
    );

     /**
     * Try and find a matching planned movement from a Z99 edit sub speciality message.
     * <p>
     * Always find planned location and hospital visit Id, then:
     * - Messages before the the same event date time
     * @param hospitalVisitId hospital visit associated with the movement
     * @param plannedLocation planned location for the movement
     * @param eventDatetime   the datetime that event was created
     * @return planned movement entities
     */
    @Query("from PlannedMovement "
            + "where hospitalVisitId = :hospitalVisitId "
            + "and (locationId = :plannedLocation or (:plannedLocation is null and locationId is null)) "
            + "and (eventDatetime <= :eventDatetime) "
            + "order by eventDatetime "
    )
    List<PlannedMovement> findMatchingMovementsFromZ99(
            HospitalVisit hospitalVisitId, Location plannedLocation, Instant eventDatetime
    );


    /**
     * Try and find a matching planned movement from a pending adt cancellation message.
     * <p>
     * Always find by the event type, planned location and hospital visit Id, then:
     * - For in order messages: message which has an earlier event date time and hasn't been cancelled, with the earliest event date time first
     * - For cancel message arriving before the original pending adt: find message which has a null eventDatetime and a cancel after current datetime
     * @param eventType       type of the planned movement
     * @param hospitalVisitId hospital visit associated with the movement
     * @param plannedLocation planned location for the movement
     * @param eventDatetime   the datetime that event was created
     * @return planned movement entities
     */
    @Query("from PlannedMovement "
            + "where eventType = :eventType and hospitalVisitId = :hospitalVisitId "
            + "and (locationId = :plannedLocation or (:plannedLocation is null and locationId is null)) "
            + "and ((eventDatetime <= :eventDatetime and cancelledDatetime is null) "
            + "      or (cancelledDatetime >= :eventDatetime and eventDatetime is null) "
            + "     )"
            + "order by cancelledDatetime, eventDatetime "
    )
    List<PlannedMovement> findMatchingMovementsFromCancel(
            String eventType, HospitalVisit hospitalVisitId, Location plannedLocation, Instant eventDatetime
    );

    List<PlannedMovement> findAllByHospitalVisitId(HospitalVisit visit);

    /**
     * For testing.
     * @param encounter      encounter string
     * @param locationString location string
     * @return optional planned movement
     */
    Optional<PlannedMovement> findByHospitalVisitIdEncounterAndLocationIdLocationString(String encounter, String locationString);

    /**
     * For testing.
     * @param encounter encounter string
     * @return list of planned movements
     */
    List<PlannedMovement> findAllByHospitalVisitIdEncounter(String encounter);

}
