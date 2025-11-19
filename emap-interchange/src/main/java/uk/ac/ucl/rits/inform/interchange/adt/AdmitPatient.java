package uk.ac.ucl.rits.inform.interchange.adt;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import uk.ac.ucl.rits.inform.interchange.EmapOperationMessageProcessingException;
import uk.ac.ucl.rits.inform.interchange.EmapOperationMessageProcessor;
import uk.ac.ucl.rits.inform.interchange.InterchangeValue;

import java.time.Instant;

/**
 * Inpatient, outpatient or emergency admission.
 * HL7 messages: A01
 */
@Data
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class AdmitPatient extends AdtMessage implements AdmissionDateTime  {
    private InterchangeValue<Instant> admissionDateTime = InterchangeValue.unknown();
    private InterchangeValue<String> admissionType = InterchangeValue.unknown();


    @Override
    public void processMessage(EmapOperationMessageProcessor processor) throws EmapOperationMessageProcessingException {
        processor.processMessage(this);
    }
}
