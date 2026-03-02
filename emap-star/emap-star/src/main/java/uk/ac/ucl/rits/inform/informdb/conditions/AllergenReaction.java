package uk.ac.ucl.rits.inform.informdb.conditions;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import uk.ac.ucl.rits.inform.informdb.TemporalCore;
import uk.ac.ucl.rits.inform.informdb.annotation.AuditTable;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.ManyToOne;
import javax.persistence.JoinColumn;
import javax.persistence.Id;
import java.time.Instant;

/**
 * \brief Reactions to allergens  that a patient can have so that it can be recognised by clinical staff.
 * <p>
 * Symptoms do not only occur in relation to diseases, but can also be used to characterise other conditions of a patient,
 * e.g. allergies.
 *
 * @author Anika Cawthorn
 * @author Tom Young
 */
@Entity
@Data
@EqualsAndHashCode(callSuper = true)
@NoArgsConstructor
@AuditTable
public class AllergenReaction extends TemporalCore<AllergenReaction, AllergenReactionAudit> {
    /**
     * \brief Unique identifier in EMAP for this allergenReaction record.
     * <p>
     * This is the primary key for the allergenReaction table.
     */
    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "emap_id_sequence")
    private long allergenReactionId;

    /**
     * \brief Identifier for the PatientCondition associated with this record.
     *
     * This is a foreign key that joins to the patientCondition table.
     */
    @ManyToOne
    @JoinColumn(name = "patientConditionId", nullable = false)
    private PatientCondition patientConditionId;

    /**
     * \brief Human readable name for this allergenReaction.
     */
    private String name;

    /**
     * Minimal information constructor.
     *
     * @param name       Name of the reaction, how it is referred to in the hospital.
     * @param condition  Patient condition
     * @param validFrom  Time at which this row was valid
     * @param storedFrom Time at which EMAP started processing this information
     */
    public AllergenReaction(String name, PatientCondition condition, Instant validFrom, Instant storedFrom) {
        this.name = name;
        this.patientConditionId = condition;
        setValidFrom(validFrom);
        setStoredFrom(storedFrom);
    }

    /**
     * Build a new PatientStateType from an existing one.
     * @param other existing PatientStateType
     */
    public AllergenReaction(AllergenReaction other) {
        super(other);
        this.allergenReactionId = other.allergenReactionId;
        this.patientConditionId = other.patientConditionId;
        this.name = other.name;
    }

    @Override
    public AllergenReaction copy() {
        return new AllergenReaction(this);
    }

    @Override
    public AllergenReactionAudit createAuditEntity(Instant validUntil, Instant storedUntil) {
        return new AllergenReactionAudit(this, validUntil, storedUntil);
    }
}
