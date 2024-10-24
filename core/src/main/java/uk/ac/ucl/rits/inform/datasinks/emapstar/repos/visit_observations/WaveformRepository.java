package uk.ac.ucl.rits.inform.datasinks.emapstar.repos.visit_observations;

import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.CrudRepository;
import uk.ac.ucl.rits.inform.informdb.visit_recordings.Waveform;

import java.time.Instant;

/**
 * Waveform repository.
 *
 * @author Jeremy Stein
 */

public interface WaveformRepository extends CrudRepository<Waveform, Long> {
    /**
     * Find waveform entries that have a known association with a location visit (ie. non-orphaned).
     * @param location location string of the location visit
     * @return all waveform entries at that location
     */
    @Query("select w from Waveform w "
            + "inner join w.locationVisitId lv "
            + "inner join lv.locationId as loc "
            + "where :location = loc.locationString "
            + "order by w.observationDatetime "
    )
    Iterable<Waveform> findAllByLocationOrderByObservationDatetime(String location);

    /**
     * Find waveform entries according to their source location, whether orphaned or not.
     * @param sourceLocation location according to the source system
     * @return all entries at that location
     */
    Iterable<Waveform> findAllBySourceLocationOrderByObservationDatetime(String sourceLocation);

    // the default delete queries are very inefficient so specify manually
    @Modifying
    @Query("delete from Waveform where observationDatetime < :observationTime")
    int deleteAllInBatchByObservationDatetimeBefore(Instant observationTime);

    @Query("select max(w.observationDatetime) from Waveform w")
    Instant mostRecentObservationDatatime();
}
