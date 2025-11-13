package uk.ac.ucl.rits.inform.informdb.decisions;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

import uk.ac.ucl.rits.inform.informdb.TemporalCore;
import uk.ac.ucl.rits.inform.informdb.annotation.AuditTable;
import uk.ac.ucl.rits.inform.informdb.identity.HospitalVisit;

import javax.persistence.Id;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import java.time.Instant;

/**
 * \brief Holds information relevant to advance decisions taken by patients.
 */
@Entity
@Data
@EqualsAndHashCode(callSuper = true)
@NoArgsConstructor
@AuditTable
public class AdvanceDecision extends TemporalCore<AdvanceDecision, AdvanceDecisionAudit> {

    /**
     * \brief Unique identifier in EMAP for this advanceDecision record.
     *
     * This is the primary key for the advanceDecision table.
     */
    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    private long advanceDecisionId;

    /**
     * \brief Identifier for the AdvanceDecisionType associated with this record.
     *
     * This is a foreign key that joins the advanceDecision table to the AdvanceDecisionType table.
     */
    @ManyToOne
    @JoinColumn(name = "advanceDecisionTypeId", nullable = false)
    private AdvanceDecisionType advanceDecisionTypeId;

    /**
     * \brief Identifier for the HospitalVisit associated with this record.
     *
     * This is a foreign key that joins the advanceDecision table to the HospitalVisit table.
     */
    @ManyToOne
    @JoinColumn(name = "hospitalVisitId", nullable = false)
    private HospitalVisit hospitalVisitId;

    /**
     * \brief Identifier used in source system for this advanceDecision.
     *
     * This identifier should be unique across all advance decisions recorded in the hospital.
     */
    @Column(nullable = false, unique = true)
    private Long internalId;

    // Optional fields for advance decisions.
    /**
     * \brief Predicate determining whether this advanceDecision was closed on discharge.
     */
    private Boolean closedDueToDischarge = false;

    /**
     * \brief Date and time at which this advanceDecision was last updated.
     */
    private Instant statusChangeDatetime;

    /**
     * \brief Date and time at which this advanceDecision was first recorded.
     */
    private Instant requestedDatetime;

     /**
      * \brief Predicate determining whether this advanceDecision has been cancelled by a user.
      */
    private Boolean cancelled = false;

    /**
     * Minimal information constructor.
     * @param advanceDecisionTypeId    Identifier of AdvanceDecisionType relevant for this AdvanceDecision.
     * @param hospitalVisitId           Identifier of HospitalVisit this AdvanceDecision has been recorded for.
     * @param internalId                Unique identifier assigned by EPIC for advance decision.
     */
    public AdvanceDecision(AdvanceDecisionType advanceDecisionTypeId, HospitalVisit hospitalVisitId,
                           Long internalId) {
        this.advanceDecisionTypeId = advanceDecisionTypeId;
        this.hospitalVisitId = hospitalVisitId;
        this.internalId = internalId;
    }

    /**
     * Build a new AdvanceDecision from an existing one.
     * @param other existing AdvanceDecision
     */
    public AdvanceDecision(AdvanceDecision other) {
        super(other);
        this.advanceDecisionId = other.advanceDecisionId;
        this.advanceDecisionTypeId = other.advanceDecisionTypeId;
        this.internalId = other.getInternalId();
        this.hospitalVisitId = other.hospitalVisitId;
        this.cancelled = other.cancelled;
        this.closedDueToDischarge = other.closedDueToDischarge;
        this.statusChangeDatetime = other.statusChangeDatetime;
        this.requestedDatetime = other.requestedDatetime;
    }

    @Override
    public AdvanceDecision copy() {
        return new AdvanceDecision(this);
    }

    @Override
    public AdvanceDecisionAudit createAuditEntity(Instant validUntil, Instant storedUntil) {
        return new AdvanceDecisionAudit(this, validUntil, storedUntil);
    }
}
