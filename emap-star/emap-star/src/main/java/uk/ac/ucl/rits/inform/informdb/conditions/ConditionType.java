package uk.ac.ucl.rits.inform.informdb.conditions;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import uk.ac.ucl.rits.inform.informdb.TemporalCore;
import uk.ac.ucl.rits.inform.informdb.annotation.AuditTable;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import java.time.Instant;

/**
 * \brief Type of condition that a patient can have.
 *
 * Types are defined by the dataType (problem list or infection) and the name (problem list-> diagnosis; infection -> infection name)
 * @author Anika Cawthorn
 * @author Stef Piatek
 */
@Entity
@Data
@EqualsAndHashCode(callSuper = true)
@NoArgsConstructor
@AuditTable
public class ConditionType extends TemporalCore<ConditionType, ConditionTypeAudit> {

    /**
     * \brief Unique identifier in EMAP for this conditionType record.
     *
     * This is the primary key for the conditionType table.
     */
    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "emap_id_sequence")
    private long conditionTypeId;

    /**
     * \brief Problem list, patient infection or allergy.
     */
    @Column(nullable = false)
    private String dataType;

    /**
     * \brief Code used within source system for this conditionType.
     */
    private String internalCode;

    /**
     * \brief Human readable name for this conditionType.
     */
    private String name;

    /**
     * \brief Subtype of the condition e.g. an allergy to food. Only populated in the context of allergies.
     */
    private String subType;

    /**
     * \brief Mapping code for the observation from the standardised vocabulary system. Not yet implemented.
     */
    private String standardisedCode;

    /**
     * \brief Nomenclature or classification system used. Not yet implemented.
     */
    private String standardisedVocabulary;

    /**
     * Minimal information constructor.
     * @param dataType   Type of patient state type; either patient infection or problem list
     * @param code       code of the patient condition type
     * @param validFrom  Timestamp from which information valid from
     * @param storedFrom Timestamp from which information stored from
     */
    public ConditionType(String dataType, String code, Instant validFrom, Instant storedFrom) {
        this.internalCode = code;
        this.dataType = dataType;
        setValidFrom(validFrom);
        setStoredFrom(storedFrom);
    }

    /**
     * Build a new PatientStateType from an existing one.
     * @param other existing PatientStateType
     */
    public ConditionType(ConditionType other) {
        super(other);
        this.conditionTypeId = other.conditionTypeId;
        this.dataType = other.dataType;
        this.internalCode = other.internalCode;
        this.name = other.name;
        this.standardisedCode = other.standardisedCode;
        this.standardisedVocabulary = other.standardisedVocabulary;
        this.subType = other.subType;
    }

    @Override
    public ConditionType copy() {
        return new ConditionType(this);
    }

    @Override
    public ConditionTypeAudit createAuditEntity(Instant validUntil, Instant storedUntil) {
        return new ConditionTypeAudit(this, validUntil, storedUntil);
    }
}
