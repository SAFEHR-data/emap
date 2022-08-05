package uk.ac.ucl.rits.inform.informdb.conditions;

import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.ManyToOne;
import javax.persistence.JoinColumn;
import javax.persistence.Id;

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
@NoArgsConstructor
public class AllergenReaction {
    /**
     * \brief Unique identifier in EMAP for this allergenReaction record.
     * <p>
     * This is the primary key for the allergenReaction table.
     */
    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
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
     */
    public AllergenReaction(String name, PatientCondition condition) {
        this.name = name;
        this.patientConditionId = condition;
    }

    /**
     * Build a new PatientStateType from an existing one.
     * @param other existing PatientStateType
     */
    public AllergenReaction(AllergenReaction other) {
        this.name = other.name;
        this.allergenReactionId = other.allergenReactionId;
        this.patientConditionId = other.patientConditionId;
    }

    public AllergenReaction copy() {
        return new AllergenReaction(this);
    }
}
