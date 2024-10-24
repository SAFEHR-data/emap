package uk.ac.ucl.rits.inform.informdb.visit_recordings;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.hibernate.annotations.Type;
import uk.ac.ucl.rits.inform.informdb.TemporalCore;
import uk.ac.ucl.rits.inform.informdb.annotation.AuditTable;
import uk.ac.ucl.rits.inform.informdb.movement.LocationVisit;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Index;
import javax.persistence.Inheritance;
import javax.persistence.InheritanceType;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.Table;
import java.time.Instant;

/**
 * \brief A waveform observation recorded about a patient.
 *
 * This is analogous to VisitObservation, but optimised for large series of
 * numerical data.
 *
 * @author Jeremy Stein
 */
@Entity
@Table(indexes = {
        @Index(name = "waveform_datetime", columnList = "observationDatetime"),
        @Index(name = "waveform_location", columnList = "sourceLocation"),
        @Index(name = "waveform_location_visit", columnList = "locationVisitId"),
})
@Data
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
@Inheritance(strategy = InheritanceType.TABLE_PER_CLASS)
@AuditTable
public class Waveform extends TemporalCore<Waveform, WaveformAudit> {

    /**
     * \brief Unique identifier for this record.
     *
     * This is the primary key.
     */
    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "waveform_id_sequence")
    private long waveformId;

    /**
     * \brief Identifier for the VisitObservationType associated with this record.
     *
     * The Waveform table is sufficiently similar in meaning to VisitObservation that it makes sense
     * to reuse VisitObservationType rather than create a new waveform metadata table.
     */
    @ManyToOne
    @JoinColumn(name = "visitObservationTypeId", nullable = false)
    private VisitObservationType visitObservationTypeId;

    /**
     * \brief Identifier for the LocationVisit associated with this record.
     *
     * If it is null, this data is said to be orphaned.
     */
    @ManyToOne
    @JoinColumn(name = "locationVisitId")
    private LocationVisit locationVisitId;

    /**
     * \brief Date and time at which this visitObservation was first made.
     * In the case of an array, this is the time of the *first* item in the array.
     *
     * The validFrom {@link TemporalCore#getValidFrom()} is the recording time, or last updated time.
     */
    @Column(columnDefinition = "timestamp with time zone", nullable = false)
    private Instant observationDatetime;

    /**
     * \brief Sampling rate of this data series in Hz.
     */
    @Column(nullable = false)
    private long samplingRate;

    /**
     * \brief Location according to the source system.
     * This will always be known,
     * and must be kept in case this data needs to be de-orphaned later.
     */
    @Column(nullable = false)
    private String sourceLocation;

    /**
     * \brief The unit of the measurement.
     *
     * For consistency with VisitObservation, the unit goes here rather than in VisitObservationType,
     * even though AIUI one waveform stream ID always uses the same unit.
     */
    @Column(nullable = false)
    private String unit;

    /**
     * \brief Value as a floating point array.
     */
    @Type(type = "uk.ac.ucl.rits.inform.informdb.visit_recordings.WaveformArray")
    @Column(columnDefinition = "DOUBLE PRECISION ARRAY", nullable = false)
    private Double[] valuesArray;

    /* unit goes in visit observation type (or equivalent table...) */

    /**
     * Default constructor.
     */
    public Waveform() {}

    /**
     * Minimal information constructor.
     * @param observationDatetime    observation datetime
     * @param validFrom              Time of the message event
     * @param storedFrom             Time that emap-core encountered the message
     */
    public Waveform(
            Instant observationDatetime,
            Instant validFrom,
            Instant storedFrom) {
        this.observationDatetime = observationDatetime;
        setValidFrom(validFrom);
        setStoredFrom(storedFrom);
    }

    /**
     * Build a new Visit observation from an existing one.
     * @param other existing visit observation
     */
    public Waveform(Waveform other) {
        super(other);
        this.waveformId = other.waveformId;
        this.visitObservationTypeId = other.visitObservationTypeId;
        this.locationVisitId = other.locationVisitId;
        this.unit = other.unit;
        this.valuesArray = other.valuesArray;
        this.observationDatetime = other.observationDatetime;
        this.samplingRate = other.samplingRate;
        this.sourceLocation = other.sourceLocation;
    }

    @Override
    public Waveform copy() {
        return new Waveform(this);
    }

    @Override
    public WaveformAudit createAuditEntity(Instant validUntil, Instant storedUntil) {
        return new WaveformAudit(this, validUntil, storedUntil);
    }
}
