package uk.ac.ucl.rits.inform.informdb.consults;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import uk.ac.ucl.rits.inform.informdb.TemporalCore;
import uk.ac.ucl.rits.inform.informdb.annotation.AuditTable;
import uk.ac.ucl.rits.inform.informdb.identity.HospitalVisit;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.Lob;
import javax.persistence.ManyToOne;
import java.time.Instant;

/**
 * \brief Holds information relevant to consultation requests for patients.
 */
@Entity
@Data
@EqualsAndHashCode(callSuper = true)
@NoArgsConstructor
@AuditTable
public class ConsultationRequest extends TemporalCore<ConsultationRequest, ConsultationRequestAudit> {

    /**
     * \brief Unique identifier in EMAP for this consultationRequest record.
     *
     * This is the primary key for the consultationRequest table.
     */
    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    private long consultationRequestId;

    /**
     * \brief Identifier for the ConsultationType associated with this record.
     *
     * This is a foreign key that joins the consultationRequest table to the ConsultationType table.
     */
    @ManyToOne
    @JoinColumn(name = "consultationTypeId", nullable = false)
    private ConsultationType consultationTypeId;

    /**
     * \brief Identifier for the HospitalVisit associated with this record.
     *
     * This is a foreign key that joins the consultationRequest table to the HospitalVisit table.
     */
    @ManyToOne
    @JoinColumn(name = "hospitalVisitId", nullable = false)
    private HospitalVisit hospitalVisitId;

    /**
     * \brief Identifier used in source system for this consultationRequest.
     */
    @Column(nullable = false)
    private Long internalId;

    /**
     * \brief Predicate determining whether this consultationRequest was closed on discharge.
     */
    private Boolean closedDueToDischarge = false;

    /**
     * \brief Notes added to the consultationRequest which are not tied to a Question.
     */
    @Lob
    private String comments;

    /**
     * \brief Date and time at which this consultationRequest was last updated.
     */
    private Instant statusChangeDatetime;

    /**
     * \brief Date and time at which this consultationRequest was scheduled.
     */
    private Instant scheduledDatetime;

    /**
     * \brief Predicate determining whether this consultationRequest has been cancelled by a user.
     */
    private Boolean cancelled = false;

    /**
     * Minimal information constructor.
     * @param consultationTypeId ID for relevant consultation type
     * @param hospitalVisitId    ID for hospital visit of patient that consultation request relates to
     * @param internalId         ID for consultation request
     */
    public ConsultationRequest(ConsultationType consultationTypeId, HospitalVisit hospitalVisitId, Long internalId) {
        this.consultationTypeId = consultationTypeId;
        this.hospitalVisitId = hospitalVisitId;
        this.internalId = internalId;
    }

    /**
     * Build a new ConsultationRequest from an existing one.
     * @param other existing ConsultationRequest
     */
    public ConsultationRequest(ConsultationRequest other) {
        super(other);
        this.consultationRequestId = other.consultationRequestId;
        this.consultationTypeId = other.consultationTypeId;
        this.hospitalVisitId = other.hospitalVisitId;
        this.internalId = other.internalId;
        this.closedDueToDischarge = other.closedDueToDischarge;
        this.comments = other.comments;
        this.statusChangeDatetime = other.statusChangeDatetime;
        this.scheduledDatetime = other.scheduledDatetime;
        this.cancelled = other.cancelled;
    }

    @Override
    public ConsultationRequest copy() {
        return new ConsultationRequest(this);
    }

    @Override
    public ConsultationRequestAudit createAuditEntity(Instant validUntil, Instant storedUntil) {
        return new ConsultationRequestAudit(this, validUntil, storedUntil);
    }
}
