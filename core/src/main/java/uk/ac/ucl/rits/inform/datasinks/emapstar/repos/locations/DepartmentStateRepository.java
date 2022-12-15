package uk.ac.ucl.rits.inform.datasinks.emapstar.repos.locations;

import org.springframework.data.repository.CrudRepository;
import uk.ac.ucl.rits.inform.informdb.movement.Department;
import uk.ac.ucl.rits.inform.informdb.movement.DepartmentState;

import java.util.List;
import java.util.Optional;

/**
 * DepartmentAudit repository.
 * @author Stef Piatek
 */
public interface DepartmentStateRepository extends CrudRepository<DepartmentState, Long> {
    Optional<DepartmentState> findFirstByDepartmentIdOrderByStoredFromDesc(Department dep);

    /**
     * For testing.
     * @param department department entity
     * @param status     status
     * @return potential department state
     */
    Optional<DepartmentState> findByDepartmentIdAndStatus(Department department, String status);

    List<DepartmentState> findByDepartmentId(Department department);

}
