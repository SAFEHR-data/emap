package uk.ac.ucl.rits.inform.interchange.adt;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import uk.ac.ucl.rits.inform.interchange.EmapOperationMessageProcessingException;
import uk.ac.ucl.rits.inform.interchange.EmapOperationMessageProcessor;
import uk.ac.ucl.rits.inform.interchange.InterchangeValue;


/**
 * Change the Sub Speciality.
 * HL7 messages: Z99
 */
@Data
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class UpdateSubSpeciality extends AdtMessage implements HospitalService {
    private InterchangeValue<String> hospitalService = InterchangeValue.unknown();
    private InterchangeValue<Long> matchedMovementId = InterchangeValue.unknown();

    @Override
    public void processMessage(EmapOperationMessageProcessor processor) throws EmapOperationMessageProcessingException {
        processor.processMessage(this);
    }

}
