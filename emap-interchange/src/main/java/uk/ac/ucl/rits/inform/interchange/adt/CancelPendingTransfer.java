package uk.ac.ucl.rits.inform.interchange.adt;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import uk.ac.ucl.rits.inform.interchange.EmapOperationMessageProcessingException;
import uk.ac.ucl.rits.inform.interchange.EmapOperationMessageProcessor;
import uk.ac.ucl.rits.inform.interchange.InterchangeValue;

import java.time.Instant;

/**
 * Cancel pending transfer event.
 * Implemented: ADT A26 (transfer)
 * Not implemented: ADT A27 (admit), ADT A25 (discharge)
 * @author Stef Piatek
 */
@Data
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class CancelPendingTransfer extends AdtMessage implements PendingEvent, AdtCancellation {
    private PendingType pendingEventType = PendingType.TRANSFER;
    private InterchangeValue<String> pendingDestination = InterchangeValue.unknown();
    private InterchangeValue<String> hospitalService = InterchangeValue.unknown();
    private Instant cancelledDateTime;


    @Override
    public void processMessage(EmapOperationMessageProcessor processor) throws EmapOperationMessageProcessingException {
        processor.processMessage(this);
    }
}
