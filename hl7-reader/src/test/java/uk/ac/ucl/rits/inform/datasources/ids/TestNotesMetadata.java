package uk.ac.ucl.rits.inform.datasources.ids;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import uk.ac.ucl.rits.inform.datasources.ids.exceptions.Hl7InconsistencyException;
import uk.ac.ucl.rits.inform.interchange.NotesMetadataMessage;
import uk.ac.ucl.rits.inform.interchange.EmapOperationMessage;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test EPIC Patient Notes metadata processing.
 * @author Sarah Keating
 */
@ActiveProfiles("test")
@SpringBootTest
public class TestNotesMetadata extends TestHl7MessageStream {
    private static final String FILE_TEMPLATE = "NotesMetadata/%s.txt";
    private static final String MRN = "40800000";
    private static final Instant START_TIME = Instant.parse("2013-02-14T09:00:00Z");
    private static final Instant EDIT_TIME = Instant.parse("2013-02-14T09:00:00Z");
    private static final Instant MSG_TIME = Instant.parse("2013-02-14T10:00:00Z");
    private static final String EPIC = "EPIC";
    private static final String DOCUMENT_TYPE = "ICU WR";
    private static final String VISIT_NUMBER = "123412341234";
    private static final Instant T08_START_TIME = Instant.parse("2021-09-01T06:00:48Z");
    private static final Instant T08_EDIT_TIME = Instant.parse("2021-09-01T22:00:00Z");
    private static final String T08_DOCUMENT_TYPE = "PROGRESS";
    

    /**
     * Read the notes from a tested HL7 message
     * 
     * @param fileName       file name to read
     * @return               a NotesMetadataMessage (an interchange)
     * @throws Exception
     */
    NotesMetadataMessage getNotesMetadata(String fileName) throws Exception {
        List<? extends EmapOperationMessage> msgs = null;
        try {
            msgs = processSingleMessage(String.format(FILE_TEMPLATE, fileName));
        } catch (Exception e) {
            throw e;
        }

        assert msgs != null;
        // filter out any implied ADT messages
        return msgs.stream()
                .filter(msg -> (msg instanceof NotesMetadataMessage))
                .map(o -> (NotesMetadataMessage) o)
                .findFirst()
                .orElseThrow();
    }

    /**
     * Given that nothing has been parsed before
     * When an TXA segment is encountered it should be parsed
     * @throws Exception shouldn't happen
     */
    @Test
    void testSingleNoteProcessed() throws Exception {
        NotesMetadataMessage notesMetadataMessage = getNotesMetadata("minimal");
        assertEquals(MRN, notesMetadataMessage.getMrn());
        assertEquals(EPIC, notesMetadataMessage.getSourceSystem());
        assertEquals(START_TIME, notesMetadataMessage.getStartedDatetime());
        assertEquals(EDIT_TIME, notesMetadataMessage.getLastEditDatetime());
        assertEquals(VISIT_NUMBER, notesMetadataMessage.getVisitNumber());
        assertEquals(DOCUMENT_TYPE, notesMetadataMessage.getNoteType());
    }

    /**
     * If the dates are missing on the TXA segment use the main date of the message.
     * @throws Exception shouldn't happen
     */
    @Test
    void testMissingDate() throws Exception {
        NotesMetadataMessage notesMetadataMessage = getNotesMetadata("missing_date");
        assertEquals(MRN, notesMetadataMessage.getMrn());
        assertEquals(EPIC, notesMetadataMessage.getSourceSystem());
        assertEquals(MSG_TIME, notesMetadataMessage.getStartedDatetime());
        assertEquals(VISIT_NUMBER, notesMetadataMessage.getVisitNumber());
        assertEquals(DOCUMENT_TYPE, notesMetadataMessage.getNoteType());
        assertNotNull(notesMetadataMessage.getLastEditDatetime());
        assertEquals(MSG_TIME, notesMetadataMessage.getLastEditDatetime());
    }

    /**
     * Given that nothing has been parsed before
     * When an TXA segment is encountered it should be parsed
     * @throws Exception shouldn't happen
     */
    @Test
    void testSingleT08NoteProcessed() throws Exception {
        NotesMetadataMessage notesMetadataMessage = getNotesMetadata("minimal_T08");
        assertEquals(MRN, notesMetadataMessage.getMrn());
        assertEquals(EPIC, notesMetadataMessage.getSourceSystem());
        assertEquals(T08_START_TIME, notesMetadataMessage.getStartedDatetime());
        assertEquals(T08_EDIT_TIME, notesMetadataMessage.getLastEditDatetime());
        assertEquals(VISIT_NUMBER, notesMetadataMessage.getVisitNumber());
        assertEquals(T08_DOCUMENT_TYPE, notesMetadataMessage.getNoteType());
    }
}
