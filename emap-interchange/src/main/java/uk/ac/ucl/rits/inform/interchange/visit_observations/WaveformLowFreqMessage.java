package uk.ac.ucl.rits.inform.interchange.visit_observations;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import uk.ac.ucl.rits.inform.interchange.EmapOperationMessageProcessingException;
import uk.ac.ucl.rits.inform.interchange.EmapOperationMessageProcessor;
import uk.ac.ucl.rits.inform.interchange.InterchangeValue;

/**
 * Data that is adjacent to Waveform data, but is not itself high-frequency data.
 * Eg. settings such as ventilation mode, and low-frequency measurements such as respiration rate.
 * At this time, waveform data doesn't come with any direct identifiers for
 * the patient, only their location.
 * @author Jeremy Stein
 */
@Data
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, property = "@class")
public class WaveformLowFreqMessage extends WaveformBaseMessage {
    /**
     * Unmapped value.
     */
    private InterchangeValue<String> sourceValue = InterchangeValue.unknown();

    /**
     * Mapped value, if it's a numerical value.
     */
    private InterchangeValue<Double> numericValue = InterchangeValue.unknown();

    /**
     * Mapped value, if it's a string. Also use for categorical, eg.  "Flow Trig"
     */
    private InterchangeValue<String> stringValue = InterchangeValue.unknown();

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
