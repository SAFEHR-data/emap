package uk.ac.ucl.rits.inform.datasinks.emapstar.repos;

import org.springframework.data.repository.CrudRepository;
import uk.ac.ucl.rits.inform.informdb.identity.HospitalVisit;
import uk.ac.ucl.rits.inform.informdb.movement.Location;
import uk.ac.ucl.rits.inform.informdb.movement.LocationVisit;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

/**
 * Visit Location repository.
 * @author Stef Piatek
 */
public interface LocationVisitRepository extends CrudRepository<LocationVisit, Long> {
    /**
     * @param visit hospital visit
     * @return the LocationVisits
     */
    List<LocationVisit> findAllByHospitalVisitId(HospitalVisit visit);

    /**
     * @param visit hospital visit
     * @return the LocationVisit wrapped in optional
     */
    Optional<LocationVisit> findByHospitalVisitIdAndDischargeDatetimeIsNull(HospitalVisit visit);


    /**
     * @param visit      hospital visit
     * @param locationId LocationId
     * @return the LocationVisit wrapped in optional
     */
    Optional<LocationVisit> findByHospitalVisitIdAndLocationIdAndDischargeDatetimeIsNull(HospitalVisit visit, Location locationId);

    /**
     * @param visit         hospital visit
     * @param locationId    Location entity
     * @param dischargeDatetime discharge time
     * @return the LocationVisit wrapped in optional
     */
    Optional<LocationVisit> findFirstByHospitalVisitIdAndLocationIdAndDischargeDatetimeLessThanEqualOrderByDischargeDatetimeDesc(
            HospitalVisit visit, Location locationId, Instant dischargeDatetime);

    /**
     * @param visit hospital visit
     * @return All location visits in descending order of admission time
     */
    List<LocationVisit> findAllByHospitalVisitIdOrderByAdmissionDatetimeDesc(HospitalVisit visit);


    /**
     * @param visit         hospital visit
     * @param locationId    location Id
     * @param admissionDatetime admission Datetime
     * @return LocationVisit wrapped in optional
     */
    Optional<LocationVisit> findByHospitalVisitIdAndLocationIdAndAdmissionDatetime(HospitalVisit visit,
                                                                                   Location locationId,
                                                                                   Instant admissionDatetime);

    /**
     * @param visit         hospital visit
     * @param locationId    location Id
     * @param dischargeDatetime admission Datetime
     * @return LocationVisit wrapped in optional
     */
    Optional<LocationVisit> findByHospitalVisitIdAndLocationIdAndDischargeDatetime(HospitalVisit visit,
                                                                                   Location locationId,
                                                                                   Instant dischargeDatetime);

    /**
     * For testing: find by location string.
     * @param location full location string
     * @return LocationVisit wrapped in optional
     */
    Optional<LocationVisit> findByLocationIdLocationString(String location);


    /**
     * For testing.
     * @param location  location string
     * @param encounter encounter string
     * @return list of all location visits
     */
    List<LocationVisit> findAllByLocationIdLocationStringAndHospitalVisitIdEncounter(String location, String encounter);

    /**
     * For testing: find location visit by the hospitalVisitId.
     * @param hospitalVisitId Id of the hospital visit
     * @return LocationVisit wrapped in optional
     */
    Optional<LocationVisit> findByDischargeDatetimeIsNullAndHospitalVisitIdHospitalVisitId(Long hospitalVisitId);

    /**
     * For testing: find location visit by the hospitalVisitId's encounter and admission time.
     * @param encounter     encounter
     * @param admissionDatetime admission Datetime
     * @return LocationVisit wrapped in optional
     */
    Optional<LocationVisit> findByHospitalVisitIdEncounterAndAdmissionDatetime(String encounter, Instant admissionDatetime);
}
