package uk.ac.ucl.rits.inform.datasinks.emapstar.repos.locations;

import org.springframework.data.repository.CrudRepository;
import uk.ac.ucl.rits.inform.informdb.movement.Bed;
import uk.ac.ucl.rits.inform.informdb.movement.Room;

/**
 * Bed repository.
 * @author Stef Piatek
 */
public interface BedRepository extends CrudRepository<Bed, Long> {
}
