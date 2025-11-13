package uk.ac.ucl.rits.inform.informdb;

import lombok.Data;

import javax.persistence.Column;
import javax.persistence.MappedSuperclass;
import java.io.Serializable;
import java.time.Instant;

/**
 * This models the common core of the temporal variables stored in almost all
 * emap star tables.
 * <p>
 * These temporal columns form the core of EMAP star's reproducibility
 * functionality. We record separately the time that this data was considered
 * true from the point of view of the hospital records (i.e. data loaded at a
 * later date may be considered true from when the original event happened), and
 * from when it was available in the database (so a repeat export of data won't
 * be contaminated by new data in reproducibility studies).
 * @param <T> The Entity Class type returned by the copy method.
 * @param <A> The Audit Entity Class type returned by the getAuditEntity method.
 * @author UCL RITS
 */
@SuppressWarnings("serial")
@Data
@MappedSuperclass
public abstract class TemporalCore<T extends TemporalCore<T, A>, A extends AuditCore> implements Serializable {

    @Column(nullable = false)
    private Instant validFrom;
    @Column(nullable = false)
    private Instant storedFrom;

    /**
     * Default constructor.
     */
    protected TemporalCore() {
    }

    /**
     * Copy constructor.
     * @param other object to copy from
     */
    protected TemporalCore(TemporalCore<?, ?> other) {
        validFrom = other.validFrom;
        storedFrom = other.storedFrom;
    }

    /**
     * Helper to set temporal from data in a single method call.
     * @param temporalFrom temporal from date
     */
    public void setTemporalFrom(TemporalFrom temporalFrom) {
        validFrom = temporalFrom.getValid();
        storedFrom = temporalFrom.getStored();
    }

    /**
     * Get the time from when this bit of data was considered true or extant.
     * Historically loaded data is back dated to its original occurrence time.
     * @return the validFrom time and date.
     */
    public Instant getValidFrom() {
        return validFrom;
    }

    /**
     * Set the time from which this fact was considered true within the hospital
     * system.
     * @param validFrom the validFrom to set
     */
    public void setValidFrom(Instant validFrom) {
        this.validFrom = validFrom;
    }

    public abstract T copy();

    /**
     * @param validUntil  the event time that invalidated the current state
     * @param storedUntil the time that star started processing the message that invalidated the current state
     * @return A new audit entity with the current state of the object.
     */
    public abstract A createAuditEntity(Instant validUntil, Instant storedUntil);
}
