package uk.ac.ucl.rits.inform.interchange.form;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import uk.ac.ucl.rits.inform.interchange.EmapOperationMessage;
import uk.ac.ucl.rits.inform.interchange.EmapOperationMessageProcessingException;
import uk.ac.ucl.rits.inform.interchange.EmapOperationMessageProcessor;

import java.io.Serializable;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

@Data
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, property = "@class")
public class FormMetadataMsg extends EmapOperationMessage implements Serializable  {

    /**
     * The form's name, eg. "UCLH ADVANCED TEP".
     */
    private String formName;

    /**
     * The form's patient friendly name, if specified.
     */
    private String formPatientFriendlyName;

    /**
     * The instant that the form started existing in its present form.
     */
    private Instant validFrom;

    /**
     * A list of IDs representing questions contained in this form, in the order
     * in which they appear, if known.
     * The ID corresponds to the sourceMessageId of FormQuestionMetadataMsg.
     */
    private List<String> questionIds = new ArrayList<>();

    /**
     * Messages must call back out to the processor (double dispatch).
     *
     * @param processor the Emap processor
     * @throws EmapOperationMessageProcessingException if message cannot be processed
     */
    @Override
    public void processMessage(EmapOperationMessageProcessor processor) throws EmapOperationMessageProcessingException {
        processor.processMessage(this);
    }
}
