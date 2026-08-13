package uk.ac.ucl.rits.inform.interchange.visit_observations;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import uk.ac.ucl.rits.inform.interchange.EmapOperationMessageProcessingException;
import uk.ac.ucl.rits.inform.interchange.EmapOperationMessageProcessor;
import uk.ac.ucl.rits.inform.interchange.InterchangeValue;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;

/**
 * Represent a high-frequency Waveform message.
 * At this time, waveform data doesn't come with any direct identifiers for
 * the patient, only their location.
 * @author Jeremy Stein
 */
@Data
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, property = "@class")
public class WaveformHighFreqMessage extends WaveformBaseMessage {
    /**
     * Channel ID according to the source system.
     */
    private String sourceChannelId;

    /**
     * Sampling rate in Hz.
     */
    private int samplingRate;

    /**
     * Numeric array.
     */
    private InterchangeValue<List<Double>> numericValues = InterchangeValue.unknown();

    /**
     * Call back to the processor so it knows what type this object is (ie. double dispatch).
     * @param processor the processor to call back to
     * @throws EmapOperationMessageProcessingException if message cannot be processed
     */
    @Override
    public void processMessage(EmapOperationMessageProcessor processor) throws EmapOperationMessageProcessingException {
        processor.processMessage(this);
    }

    /**
     * @return expected observation datetime for the next message, if it exists and there are
     * no gaps between messages
     */
    @JsonIgnore
    public Instant getExpectedNextObservationDatetime() {
        int numValues = numericValues.get().size();
        long microsToAdd = 1_000_000L * numValues / samplingRate;
        return getObservationTime().plus(microsToAdd, ChronoUnit.MICROS);
    }

}
