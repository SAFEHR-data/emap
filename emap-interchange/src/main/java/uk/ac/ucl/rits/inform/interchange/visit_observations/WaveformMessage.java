package uk.ac.ucl.rits.inform.interchange.visit_observations;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import uk.ac.ucl.rits.inform.interchange.EmapOperationMessage;
import uk.ac.ucl.rits.inform.interchange.EmapOperationMessageProcessingException;
import uk.ac.ucl.rits.inform.interchange.EmapOperationMessageProcessor;
import uk.ac.ucl.rits.inform.interchange.InterchangeValue;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;

/**
 * Represent a Waveform message. At this time, waveform data doesn't come with any direct identifiers for
 * the patient, only their location.
 * @author Jeremy Stein
 */
@Data
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, property = "@class")
public class WaveformMessage extends EmapOperationMessage {
    private String sourceObservationType = "waveform";

    /**
     * Time of the observation.
     */
    private Instant observationTime;

    /**
     * Location string according to the original data source.
     */
    private String sourceLocationString;

    /**
     * Location string, mapped by the data source to the canonical Emap format,
     * which matches what we get from the main HL7 ADT feed.
     */
    private String mappedLocationString;

    /**
     * Stream ID according to the source system.
     */
    private String sourceStreamId;

    /**
     * Stream description mapped by the data source.
     */
    private String mappedStreamDescription;

    /**
     * Sampling rate in Hz.
     */
    private int samplingRate;

    /**
     * Unit of the measurement.
     */
    private String unit;

    /**
     * Numeric value.
     */
    private InterchangeValue<List<Double>> numericValues = InterchangeValue.unknown();

    /**
     * @return expected observation datetime for the next message, if it exists and there are
     * no gaps between messages
     */
    @JsonIgnore
    public Instant getExpectedNextObservationDatetime() {
        int numValues = numericValues.get().size();
        long microsToAdd = 1_000_000L * numValues / samplingRate;
        return observationTime.plus(microsToAdd, ChronoUnit.MICROS);
    }

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
