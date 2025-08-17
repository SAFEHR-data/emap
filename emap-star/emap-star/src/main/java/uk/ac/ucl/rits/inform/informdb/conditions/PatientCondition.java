package uk.ac.ucl.rits.inform.informdb.conditions;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;
import uk.ac.ucl.rits.inform.informdb.TemporalCore;
import uk.ac.ucl.rits.inform.informdb.annotation.AuditTable;
import uk.ac.ucl.rits.inform.informdb.identity.HospitalVisit;
import uk.ac.ucl.rits.inform.informdb.identity.Mrn;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Index;
import javax.persistence.JoinColumn;
import javax.persistence.Lob;
import javax.persistence.ManyToOne;
import javax.persistence.Table;
import java.time.Instant;
import java.time.LocalDate;

/**
 * \brief Represents patient conditions that start and can end.
 * <p>
 * Currently envisaged as storing infection control's patient infection information and problems from problem lists.
 * @author Anika Cawthorn
 * @author Stef Piatek
 */
@Entity
@Data
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
@NoArgsConstructor
@Table(indexes = {@Index(name = "pc_internal_id", columnList = "internalId")})
@AuditTable
public class PatientCondition extends TemporalCore<PatientCondition, PatientConditionAudit> {

    /**
     * \brief Unique identifier in EMAP for this patientCondition record.
     * <p>
     * This is the primary key for the patientCondition table.
     */
    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    private long patientConditionId;

    /**
     * \brief Identifier for the ConditionType associated with this record.
     * <p>
     * This is a foreign key that joins the patientCondition table to the ConditionType table.
     */
    @ManyToOne
    @JoinColumn(name = "conditionTypeId", nullable = false)
    private ConditionType conditionTypeId;

    /**
     * \brief Identifier used in source system for this patientCondition.
     */
    private Long internalId;

    /**
     * \brief Identifier for the Mrn associated with this record.
     * <p>
     * This is a foreign key that joins the patientCondition table to the Mrn table.
     */
    @ManyToOne
    @JoinColumn(name = "mrnId", nullable = false)
    private Mrn mrnId;

    /**
     * \brief Identifier for the HospitalVisit associated with this record.
     * <p>
     * This is a foreign key that joins the patientCondition table to the HospitalVisit table.
     */
    @ManyToOne
    @JoinColumn(name = "hospitalVisitId")
    private HospitalVisit hospitalVisitId;

    /**
     * \brief Date and time at which this patientCondition was added to the record.
     * <p>
     * It will only ever addedDatetime OR addedDate depending on the granularity available for the kind of
     * condition, i.e. whether it is a problem list, allergy or infection.
     */
    private Instant addedDatetime;

    /**
     * \brief Date at which this patientCondition was added to the record.
     * <p>
     * It will only ever addedDatetime OR addedDate depending on the granularity available for the kind of
     * condition, i.e. whether it is a problem list, allergy or infection.
     */
    private LocalDate addedDate;
    /**
     * \brief Date and time at which this patientCondition was resolved.
     * <p>
     * It will only ever resolutionDatetime OR resolutionDate depending on the granularity available for the kind of
     * condition, i.e. whether it is a problem list, allergy or infection.
     */
    private Instant resolutionDatetime;

    /**
     * \brief Date at which this patientCondition was resolved.
     * <p>
     * It will only ever resolutionDatetime OR resolutionDate depending on the granularity available for the kind of
     * condition, i.e. whether it is a problem list, allergy or infection.
     */
    private LocalDate resolutionDate;

    /**
     * \brief Date at which the patientCondition started (if known).
     */
    private LocalDate onsetDate;

    /**
     * \brief Problem List classification (e.g. Temporary).
     */
    private String classification;

    /**
     * \brief Status of patientCondition.
     * <p>
     * Is this active, resolved, expired etc.
     */
    private String status;

    /**
     * \brief condition priority.
     */
    private String priority;

    /**
     * \brief Is this condition deleted.
     */
    private Boolean isDeleted = false;

    /**
     * \brief Comments added by clinician.
     */
    @Lob
    private String comment;

    /**
     * \brief Description of how severe this condition is.
     */
    private String severity;

    /**
     * Minimal information constructor.
     * @param conditionTypeId ID for patient state type
     * @param mrn             patient ID
     * @param conditionId     identifier used for condition in the hospital
     */
    public PatientCondition(ConditionType conditionTypeId, Mrn mrn, Long conditionId) {
        this.conditionTypeId = conditionTypeId;
        this.mrnId = mrn;
        this.internalId = conditionId;
    }

    /**
     * Build a new PatientState from an existing one.
     * @param other existing PatientState
     */
    public PatientCondition(PatientCondition other) {
        super(other);
        patientConditionId = other.patientConditionId;
        conditionTypeId = other.conditionTypeId;
        mrnId = other.mrnId;
        if (other.hospitalVisitId != null) {
            hospitalVisitId = other.hospitalVisitId;
        }
        internalId = other.internalId;
        addedDate = other.addedDate;
        addedDatetime = other.addedDatetime;
        resolutionDate = other.resolutionDate;
        resolutionDatetime = other.resolutionDatetime;
        onsetDate = other.onsetDate;
        classification = other.classification;
        status = other.status;
        priority = other.priority;
        comment = other.comment;
        isDeleted = other.isDeleted;
        severity = other.severity;
    }

    @Override
    public PatientCondition copy() {
        return new PatientCondition(this);
    }

    @Override
    public PatientConditionAudit createAuditEntity(Instant validUntil, Instant storedUntil) {
        return new PatientConditionAudit(this, validUntil, storedUntil);
    }
}
