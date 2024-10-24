package uk.ac.ucl.rits.inform.datasinks.emapstar.repos.visit_observations;

import org.springframework.data.repository.CrudRepository;
import uk.ac.ucl.rits.inform.informdb.visit_recordings.VisitObservationType;

import java.util.List;
import java.util.Optional;

/**
 * Visit Observation Type repository.
 *
 * @author Stef Piatek
 * @author Anika Cawthorn
 */
public interface VisitObservationTypeRepository extends CrudRepository<VisitObservationType, Long> {
    /**
     * Query visit observation type by required fields.
     * If interface identifier is not null, it will search by this. If not, it will search for the identifier in application.
     *
     * @param interfaceId     Identifier for visit observation type in EPIC
     * @param idInApplication Hospital flowsheet identifier
     * @param observationType type of observation from the source system
     * @return optional visit observation type
     */
    default Optional<VisitObservationType> find(
            String interfaceId, String idInApplication, String observationType) {
        if (interfaceId != null) {
            return findByInterfaceIdAndSourceObservationType(interfaceId, observationType);
        }
        return findByIdInApplicationAndSourceObservationType(idInApplication, observationType);
    }

    /**
     * @param interfaceId           Interface identifier used for search
     * @param sourceObservationType Observation type used for search
     * @return VisitObservationType
     */
    Optional<VisitObservationType> findByInterfaceIdAndSourceObservationType(String interfaceId, String sourceObservationType);

    /**
     * @param idInApplication       Identifier in application used for search
     * @param sourceObservationType Observation type used for search
     * @return VisitObservationType
     */
    Optional<VisitObservationType> findByIdInApplicationAndSourceObservationType(String idInApplication, String sourceObservationType);

    /**
     * Finds a Visit Observation Type that is specified by both an interface identifier and an internal id.
     *
     * @param interfaceId     Identifier for VisitObservationType.
     * @param idInApplication IdInApplication for VisitObservationType.
     * @return Visit Observation Type with interfaceId and idInApplication as specified, if exists.
     */
    Optional<VisitObservationType> findByInterfaceIdAndIdInApplication(String interfaceId, String idInApplication);

    /**
     * For testing.
     * @param sourceObservationType source observation type (eg. "waveform")
     * @return all rows matching
     */
    List<VisitObservationType> findAllBySourceObservationType(String sourceObservationType);
}
