package uk.ac.ucl.rits.inform.informdb.movement;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import uk.ac.ucl.rits.inform.informdb.AuditCore;
import uk.ac.ucl.rits.inform.informdb.TemporalFrom;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.Table;
import java.time.Instant;

/**
 * \brief Represents the state of a given Bed.
 */
@SuppressWarnings("serial")
@Entity
@Table
@Data
@NoArgsConstructor
@EqualsAndHashCode(callSuper = true)
public class BedState extends AuditCore<BedState> {

    /**
     * \brief Unique identifier in EMAP for this bedState record.
     *
     * This is the primary key for the bedState table.
     */
    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "emap_id_sequence")
    private Long bedStateId;

    /**
     * \brief Identifier for the Bed associated with this record.
     *
     * This is a foreign key that joins the bedState table to the Bed table.
     */
    @ManyToOne
    @JoinColumn(name = "bedId", nullable = false)
    private Bed bedId;

    /**
     * \brief Indentifier ??
     */
    @Column(nullable = false, unique = true)
    private Long csn;

    /**
     * \brief Predicate determining whether the Bed is in census.
     */
    private Boolean isInCensus;

    /**
     * \brief Predicate determining whether the Bed is a bunk.
     */
    private Boolean isBunk;

    /**
     * \brief Current status of the Bed.
     */
    private String status;

    /**
     * \brief Pool bed count ??
     */
    private Long poolBedCount;

    /**
     * Create Bed State.
     * @param bedId        parent bed id
     * @param csn          CSN for contact
     * @param isInCensus   is bed in census
     * @param isBunk       is it a bunk bed
     * @param status       status
     * @param isPool       is it a pool bed
     * @param temporalFrom temporal from information
     */
    public BedState(Bed bedId, Long csn, Boolean isInCensus, Boolean isBunk, String status, Boolean isPool, TemporalFrom temporalFrom) {
        this.bedId = bedId;
        this.csn = csn;
        this.isInCensus = isInCensus;
        this.isBunk = isBunk;
        this.status = status;
        if (isPool) {
            // will increment pool bed upon updating so initialise as 0 only if it's a pool bed
            poolBedCount = 0L;
        }
        setValidFrom(temporalFrom.getValid());
        setStoredFrom(temporalFrom.getStored());
    }

    public void incrementPoolBedCount() {
        poolBedCount += 1;
    }

    private BedState(BedState other) {
        super(other);
        setValidFrom(other.getValidFrom());
        setStoredFrom(other.getStoredFrom());
        this.bedStateId = other.bedStateId;
        this.bedId = other.bedId;
        this.csn = other.csn;
        this.isInCensus = other.isInCensus;
        this.isBunk = other.isBunk;
        this.poolBedCount = other.poolBedCount;
        this.status = other.status;
    }

    @Override
    public BedState copy() {
        return new BedState(this);
    }

    /**
     * @param validUntil  the event time that invalidated the current state
     * @param storedUntil the time that star started processing the message that invalidated the current state
     * @return A new audit entity with the current state of the object.
     */
    @Override
    public BedState createAuditEntity(Instant validUntil, Instant storedUntil) {
        BedState audit = copy();
        audit.setValidUntil(validUntil);
        audit.setStoredUntil(storedUntil);
        return audit;
    }
}
