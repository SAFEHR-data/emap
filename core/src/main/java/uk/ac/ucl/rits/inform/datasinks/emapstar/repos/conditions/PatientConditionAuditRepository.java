package uk.ac.ucl.rits.inform.datasinks.emapstar.repos.conditions;

import org.springframework.data.repository.CrudRepository;
import uk.ac.ucl.rits.inform.informdb.conditions.PatientConditionAudit;

import java.util.Optional;

/**
 * PatientConditionAudit repository.
 * @author Stef Piatek
 * @author Tom Young
 */
public interface PatientConditionAuditRepository extends CrudRepository<PatientConditionAudit, Long> {

    /**
     * For testing.
     * @param patientConditionId patient condition Id
     * @return potential patient condition audit entity
     */
    Optional<PatientConditionAudit> findByPatientConditionId(long patientConditionId);
}
