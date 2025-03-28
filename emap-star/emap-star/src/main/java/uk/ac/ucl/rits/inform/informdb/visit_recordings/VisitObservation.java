package uk.ac.ucl.rits.inform.informdb.visit_recordings;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import uk.ac.ucl.rits.inform.informdb.TemporalCore;
import uk.ac.ucl.rits.inform.informdb.annotation.AuditTable;
import uk.ac.ucl.rits.inform.informdb.identity.HospitalVisit;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Index;
import javax.persistence.Inheritance;
import javax.persistence.InheritanceType;
import javax.persistence.JoinColumn;
import javax.persistence.Lob;
import javax.persistence.ManyToOne;
import javax.persistence.Table;
import java.time.Instant;
import java.time.LocalDate;

/**
 * \brief An observation recorded about patient.
 *
 * VisitObservations represent discrete nurse (or machine) recodred observations
 * about patients at specific time points.
 * @author Roma Klapaukh
 * @author Stef Piatek
 * @author Anika Cawthorn
 */
@Entity
@Table(indexes = {@Index(name = "vo_hospital_visit_id", columnList = "hospitalVisitId"),
        @Index(name = "vo_visit_observation_type", columnList = "visitObservationTypeId"),
        @Index(name = "vo_observation_datetime", columnList = "observationDatetime")})
@Data
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
@Inheritance(strategy = InheritanceType.TABLE_PER_CLASS)
@AuditTable
public class VisitObservation extends TemporalCore<VisitObservation, VisitObservationAudit> {

    /**
     * \brief Unique identifier in EMAP for this visitObservation record.
     *
     * This is the primary key for the visitObservation table.
     */
    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    private long visitObservationId;

    /**
     * \brief Identifier for the VisitObservationType associated with this record.
     *
     * This is a foreign key that joins the visitObservation table to the VisitObservationType table.
     */
    @ManyToOne
    @JoinColumn(name = "visitObservationTypeId", nullable = false)
    private VisitObservationType visitObservationTypeId;

    /**
     * \brief Identifier for the HospitalVisit associated with this record.
     *
     * This is a foreign key that joins the visitObservation table to the HospitalVisit table.
     */
    @ManyToOne
    @JoinColumn(name = "hospitalVisitId", nullable = false)
    private HospitalVisit hospitalVisitId;

    /**
     * \brief Date and time at which this visitObservation was first made.
     *
     * The validFrom {@link TemporalCore#getValidFrom()} is the recording time, or last updated time.
     */
    @Column(nullable = false)
    private Instant observationDatetime;

    /**
     * \brief Value as text.
     */
    @Lob
    private String valueAsText;

    /**
     * \brief Value as a number.
     */
    private Double valueAsReal;

    /**
     * \brief Value as a date.
     */
    private LocalDate valueAsDate;

    /**
     * \brief Units of the visitObservation.
     */
    private String unit;


    /**
     * \brief Comments added by clinician.
     */
    @Lob
    private String comment;

    /**
     * \brief The hospital system that emap received the data from (caboodle or EPIC).
     */
    @Column(nullable = false)
    private String sourceSystem;

    /**
     * Default constructor.
     */
    public VisitObservation() {}

    /**
     * Minimal information constructor.
     * @param hospitalVisitId        hospital visit
     * @param visitObservationTypeId visit observation type
     * @param observationDatetime    observation datetime
     * @param sourceSystem           the system that last changed visit observation information
     * @param validFrom              Time of the message event
     * @param storedFrom             Time that emap-core encountered the message
     */
    public VisitObservation(HospitalVisit hospitalVisitId, VisitObservationType visitObservationTypeId,
                            Instant observationDatetime, String sourceSystem, Instant validFrom, Instant storedFrom) {
        this.visitObservationTypeId = visitObservationTypeId;
        this.hospitalVisitId = hospitalVisitId;
        this.observationDatetime = observationDatetime;
        this.sourceSystem = sourceSystem;
        setValidFrom(validFrom);
        setStoredFrom(storedFrom);
    }

    /**
     * Build a new Visit observation from an existing one.
     * @param other existing visit observation
     */
    public VisitObservation(VisitObservation other) {
        super(other);
        this.visitObservationId = other.visitObservationId;
        this.visitObservationTypeId = other.visitObservationTypeId;
        this.hospitalVisitId = other.hospitalVisitId;
        this.valueAsText = other.valueAsText;
        this.valueAsReal = other.valueAsReal;
        this.valueAsDate = other.valueAsDate;
        this.unit = other.unit;
        this.comment = other.comment;
        this.observationDatetime = other.observationDatetime;
        this.sourceSystem = other.sourceSystem;
    }

    @Override
    public VisitObservation copy() {
        return new VisitObservation(this);
    }

    @Override
    public VisitObservationAudit createAuditEntity(Instant validUntil, Instant storedUntil) {
        return new VisitObservationAudit(this, validUntil, storedUntil);
    }
}
