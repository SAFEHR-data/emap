package uk.ac.ucl.rits.inform.interchange;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.io.Serializable;
import java.time.Instant;


/**
 * Interchange format of a MetadataNotes message.
 * @author Sarah Keating
 */
@Data
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, property = "@class")
public class NotesMetadataMessage extends EmapOperationMessage implements Serializable {
    /**
     * Unique epic ID for notesMetadata.
     */
    private Long notesMetadataId;

    /**
     * Patient ID this notes relates to.
     */
    private String mrn;

    /**
     * Hospital visit of the patient that this note metadata relates to.
     */
    private String visitNumber;

    /**
     * Notes metadata type name used by EPIC.
     * e.g. ICU
     */
     private String noteType;

    /**
     * initial time.
     */
    private Instant startedDatetime;

    /**
     * last edited time.
     */
    private Instant lastEditDatetime;

    /**
     * role a person making the note.
     */
    private String editorRole;


    /**
     * Call back to the processor so it knows what type this object is (ie. double dispatch).
     * @param processor the processor to call back to
     * @throws EmapOperationMessageProcessingException if message cannot be processed
     */
    @Override
    public void processMessage(EmapOperationMessageProcessor processor) throws EmapOperationMessageProcessingException {
        processor.processMessage(this);
    }
}
