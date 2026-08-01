package uk.ac.ucl.rits.inform.datasinks.emapstar.repos;

import org.springframework.data.repository.CrudRepository;
import uk.ac.ucl.rits.inform.informdb.notes.NotesMetadataAudit;

import java.util.List;

/**
 * Interaction with the NotesMetadataAudit table.
 * @author Sarah Keating
 */
public interface NotesMetadataAuditRepository extends CrudRepository<NotesMetadataAudit, Long> {
    /**
     * For testing.
     * @param hospitalVisitId id of the hospital visit
     * @return all notes metadata audits
     */
    List<NotesMetadataAudit> findAllByHospitalVisitId(long hospitalVisitId);
}
