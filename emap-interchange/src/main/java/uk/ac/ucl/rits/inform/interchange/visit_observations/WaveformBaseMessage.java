package uk.ac.ucl.rits.inform.interchange.visit_observations;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import uk.ac.ucl.rits.inform.interchange.EmapOperationMessage;

import java.time.Instant;

/**
 * Represent a Waveform message, which may be a message carrying actual
 * high-frequency data (see {@link WaveformMessage}) or lower frequency settings and measurements.
 * At this time, waveform data doesn't come with any direct identifiers for the patient, only their location.
 * @author Jeremy Stein
 */
@Data
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, property = "@class")
public abstract class WaveformBaseMessage extends EmapOperationMessage {
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
     * Do we want to be more specific here? Eg. carescape, etc
     * Eg. get from the CSV metadata and prefix with "waveform-"
     */
    private String sourceObservationType = "waveform";

    /**
     * Variable ID according to the source system.
     * Has previously been referred to as stream ID, so you may see that in some places.
     */
    private String sourceVariableId;

    /**
     * Variable (aka. stream) description mapped by the data source.
     */
    private String mappedVariableDescription;

    /**
     * Unit of the measurement.
     */
    private String unit;

}
