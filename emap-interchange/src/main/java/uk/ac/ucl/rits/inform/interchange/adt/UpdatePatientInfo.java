package uk.ac.ucl.rits.inform.interchange.adt;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import uk.ac.ucl.rits.inform.interchange.EmapOperationMessageProcessingException;
import uk.ac.ucl.rits.inform.interchange.EmapOperationMessageProcessor;

/**
 * Change patient demographics, can contain visit information.
 * HL7 messages: A08, A28, A31
 */
@Data
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class UpdatePatientInfo extends AdtMessage {
    @Override
    public void processMessage(EmapOperationMessageProcessor processor) throws EmapOperationMessageProcessingException {
        processor.processMessage(this);
    }
}
