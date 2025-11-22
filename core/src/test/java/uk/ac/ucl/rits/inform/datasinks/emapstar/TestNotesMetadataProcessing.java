package uk.ac.ucl.rits.inform.datasinks.emapstar;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import uk.ac.ucl.rits.inform.datasinks.emapstar.repos.HospitalVisitRepository;
import uk.ac.ucl.rits.inform.datasinks.emapstar.repos.MrnRepository;
import uk.ac.ucl.rits.inform.datasinks.emapstar.repos.NotesMetadataRepository;
import uk.ac.ucl.rits.inform.informdb.notes.NotesMetadata;
import uk.ac.ucl.rits.inform.informdb.identity.HospitalVisit;
import uk.ac.ucl.rits.inform.informdb.identity.Mrn;
import uk.ac.ucl.rits.inform.informdb.identity.MrnToLive;
import uk.ac.ucl.rits.inform.interchange.NotesMetadataMessage;
import uk.ac.ucl.rits.inform.interchange.EmapOperationMessageProcessingException;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test cases to assert correct processing of NotesMetadataMessages.
 * @author Anika Cawthorn
 */
public class TestNotesMetadataProcessing extends MessageProcessingBase {
    @Autowired
    NotesMetadataRepository notesMetadataRepo;
    @Autowired
    HospitalVisitRepository hospitalVisitRepository;
    @Autowired
    MrnRepository mrnRepository;

    private NotesMetadataMessage minimal;
    // private NotesMetadataMessage minimalWithQuestions;
    // private NotesMetadataMessage closedAtDischarge;
    // private NotesMetadataMessage cancelled;

    private static Instant NOTES_METADATA_START_TIME =  Instant.parse("2013-02-14T09:00:00Z");
    private static Instant NOTES_METADATA_EDIT_TIME =  Instant.parse("2013-02-14T09:00:00Z");
    private static Long NOTES_METADATA_INTERNAL_ID = 1234521112L;
    private static String NOTES_METADATA_MRN = "40800000";

    @BeforeEach
    public void setUp() throws IOException {
        minimal = messageFactory.getNotesMetadataMessage("minimal.yaml");
    }

    /**
     *  Given that no MRNS or hospital visits exist in the database
     *  When a new NotesMetadataMessage without questions arrives
     *  Then a minimal HospitalVisit, Mrn and NotesMetadata should be created
     */
    @Test
    void testMinimalNotesMetadataCreated() throws EmapOperationMessageProcessingException {
        processSingleMessage(minimal);

        List<Mrn> mrns = getAllMrns();
        assertEquals(1, mrns.size());

        assertEquals(NOTES_METADATA_MRN, mrns.get(0).getMrn());
        MrnToLive mrnToLive = mrnToLiveRepo.getByMrnIdEquals(mrns.get(0));
        assertNotNull(mrnToLive);

        Optional<HospitalVisit> visit = hospitalVisitRepository.findByEncounter(defaultEncounter);
        assertTrue(visit.isPresent());



        NotesMetadata notesMetadata = notesMetadataRepo.findByInternalId(NOTES_METADATA_INTERNAL_ID).orElseThrow();
        assertEquals(NOTES_METADATA_EDIT_TIME, notesMetadata.getValidFrom());
        assertEquals(NOTES_METADATA_START_TIME, notesMetadata.getStartedDatetime());
    }
}
