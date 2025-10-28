package uk.ac.ucl.rits.inform.informdb.identity;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import uk.ac.ucl.rits.inform.informdb.TemporalCore;
import uk.ac.ucl.rits.inform.informdb.annotation.AuditTable;
import uk.ac.ucl.rits.inform.informdb.visit_recordings.VisitObservation;

import javax.persistence.CascadeType;
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
import javax.persistence.OneToMany;
import javax.persistence.Table;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * \brief This a single visit to the hospital.
 *
 * This is not necessarily an inpatient visit, but includes outpatients, imaging, etc.
 * @author UCL RITS
 */
@SuppressWarnings("serial")
@Entity
@Data
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
@Table(indexes = {
    @Index(name = "encounterIndex", columnList = "encounter", unique = false),
    @Index(name = "admissionDatetimeIndex", columnList = "admissionDatetime"),
    @Index(name = "dischargeDatetimeIndex", columnList = "dischargeDatetime")
})
@Inheritance(strategy = InheritanceType.TABLE_PER_CLASS)
@AuditTable
public class HospitalVisit extends TemporalCore<HospitalVisit, HospitalVisitAudit> {

    /**
     * \brief Unique identifier in EMAP for this hospitalVisit record.
     *
     * This is the primary key for the HospitalVisit table.
     */
    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    private Long hospitalVisitId;

    /**
     * \brief Identifier for the Mrn associated with this record.
     *
     * This is a foreign key that joins the hospitalVisit table to the Mrn table.
     */
    @ManyToOne
    @JoinColumn(name = "mrnId", nullable = false)
    private Mrn mrnId;

    /**
     * \brief The source system from which we learnt about this hospital visit.
     */
    @Column(nullable = false)
    private String sourceSystem;

    /**
     * \brief Date and time at which this hospitalVisit was first recorded.
     *
     * patient was first seen in the hospital as part of this visit.
     * This may be prior to their admission.
     */
    @Column(columnDefinition = "timestamp with time zone")
    private Instant presentationDatetime;

    /**
     * \brief Date and time at which this hospitalVisit formally began.
     *
     * at which the patient was formally admitted.
     */
    @Column(columnDefinition = "timestamp with time zone")
    private Instant admissionDatetime;

    /**
     * \brief the admission type for this hospitalVisit.
     */
    private String admissionType;

    /**
     * \brief Date and time at which this hospitalVisit formally ended.
     *
     * at which the patient was discharged.
     */
    @Column(columnDefinition = "timestamp with time zone")
    private Instant dischargeDatetime;

    /**
     * \brief The patient class. E.g. Inpatient or Outpaitent.
     */
    private String patientClass;

    /**
     * \brief The patient's arrival method at hospital.
     */
    private String arrivalMethod;

    /**
     * \brief Where the patient went after their departure.
     */
    private String dischargeDestination;

    /**
     * \brief The patient's disposition on departure.
     */
    private String dischargeDisposition;

    /**
     * \brief The source system identifier of this hospital visit.
     *
     * In Epic this corresponds to the CSN.
     */
    @Column(nullable = false, unique = true)
    private String encounter;

    //TODO Exclude from doxygen
    /**
     * Visit observations should be deleted if an encounter is deleted.
     */
    @ToString.Exclude
    @OneToMany(targetEntity = VisitObservation.class, mappedBy = "hospitalVisitId", orphanRemoval = true, cascade = CascadeType.ALL)
    private List<VisitObservation> visitObservations = new ArrayList<>();

    /**
     * Default constructor.
     */
    public HospitalVisit() {}

    /**
     * Build a hospital visit from an existing object.
     * @param other hospital visit
     */
    private HospitalVisit(HospitalVisit other) {
        super(other);
        this.hospitalVisitId = other.hospitalVisitId;
        this.encounter = other.encounter;
        this.admissionDatetime = other.admissionDatetime;
        this.admissionType = other.admissionType;
        this.arrivalMethod = other.arrivalMethod;
        this.dischargeDestination = other.dischargeDestination;
        this.dischargeDisposition = other.dischargeDisposition;
        this.dischargeDatetime = other.dischargeDatetime;
        this.mrnId = other.mrnId;
        this.patientClass = other.patientClass;
        this.presentationDatetime = other.presentationDatetime;
        this.sourceSystem = other.sourceSystem;
        this.visitObservations = other.visitObservations;
    }

    @Override
    public HospitalVisit copy() {
        return new HospitalVisit(this);
    }

    @Override
    public HospitalVisitAudit createAuditEntity(Instant validUntil, Instant storedUntil) {
        return new HospitalVisitAudit(this, validUntil, storedUntil);
    }
}
