package uk.ac.ucl.rits.inform.datasinks.emapstar.repos;

import org.springframework.data.repository.CrudRepository;

import uk.ac.ucl.rits.inform.informdb.identity.HospitalVisit;
import uk.ac.ucl.rits.inform.informdb.notes.NotesMetadata;

import java.util.List;
import java.util.Optional;

/**
 * Interaction with the NotesMetadata table.
 * @author Sarah Keating
 */
public interface NotesMetadataRepository extends CrudRepository<NotesMetadata, Long> {
    /**
     * Find notes metadata by unique identifier.
     * @param internalId internal ID for the notes metadata
     * @return possible NotesMetadata
     */
    Optional<NotesMetadata> findByInternalId(Long internalId);

    List<NotesMetadata> findAllByHospitalVisitId(HospitalVisit hospitalVisit);
}
