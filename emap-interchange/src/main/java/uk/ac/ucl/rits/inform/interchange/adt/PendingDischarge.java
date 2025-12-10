package uk.ac.ucl.rits.inform.interchange.adt;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import uk.ac.ucl.rits.inform.interchange.EmapOperationMessageProcessingException;
import uk.ac.ucl.rits.inform.interchange.EmapOperationMessageProcessor;
import uk.ac.ucl.rits.inform.interchange.InterchangeValue;

/**
 * Pending Dishcharge event.
 * Implemented: ADT A16 (Pending Discharge)
 * Not implemented: ADT A14 (Pending admit)
 * @author Steve Thompson
 */
@Data
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class PendingDischarge extends AdtMessage implements PendingEvent {
    private PendingType pendingEventType = PendingType.DISCHARGE;
    private InterchangeValue<String> pendingDestination = InterchangeValue.unknown();
    private InterchangeValue<String> hospitalService = InterchangeValue.unknown();

    @Override
    public void processMessage(EmapOperationMessageProcessor processor) throws EmapOperationMessageProcessingException {
        processor.processMessage(this);
    }
}
