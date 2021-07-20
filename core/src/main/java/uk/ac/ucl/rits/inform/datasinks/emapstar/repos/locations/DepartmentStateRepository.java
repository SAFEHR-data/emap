package uk.ac.ucl.rits.inform.datasinks.emapstar.repos.locations;

import org.springframework.data.repository.CrudRepository;
import uk.ac.ucl.rits.inform.informdb.movement.DepartmentState;

/**
 * DepartmentAudit repository.
 * @author Stef Piatek
 */
public interface DepartmentStateRepository extends CrudRepository<DepartmentState, Long> {
}
