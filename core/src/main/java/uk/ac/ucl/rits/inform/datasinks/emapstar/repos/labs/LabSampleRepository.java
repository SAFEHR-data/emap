package uk.ac.ucl.rits.inform.datasinks.emapstar.repos.labs;

import org.springframework.data.repository.CrudRepository;
import uk.ac.ucl.rits.inform.informdb.identity.Mrn;
import uk.ac.ucl.rits.inform.informdb.labs.LabSample;

import java.util.Optional;

/**
 * Lab Sample repository.
 * @author Stef Piatek
 */
public interface LabSampleRepository extends CrudRepository<LabSample, Long> {
    Optional<LabSample> findByMrnIdAndExternalLabNumber(Mrn mrn, String externalLabNumber);

    /**
     * For testing.
     * @param labNumber external lab number
     * @return possible LabCollection
     */
    Optional<LabSample> findByExternalLabNumber(String labNumber);
}
