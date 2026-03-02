package uk.ac.ucl.rits.inform.informdb.forms;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import uk.ac.ucl.rits.inform.informdb.TemporalCore;
import uk.ac.ucl.rits.inform.informdb.TemporalFrom;
import uk.ac.ucl.rits.inform.informdb.annotation.AuditTable;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import java.time.Instant;

/**
 * \brief Represents the Question in a form (basically the metadata for a form answer).
 * It may not literally be a question, eg. "Limb", you can think of that as a prompt.
 */
@Entity
@Data
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
@AuditTable
public class FormQuestion extends TemporalCore<FormQuestion, FormQuestionAudit> {
    /**
     * \brief Unique identifier in EMAP.
     */
    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "emap_id_sequence")
    private Long formQuestionId;

    /**
     * \brief String identifier from the source system of the question.
     * .
     */
    @Column(nullable = false, unique = true)
    private String internalId;

    /**
     * \brief Name of the concept (ie. the "Question")
     * .
     */
    private String conceptName;

    /**
     * \brief Abbreviated concept name (ie. the "Question")
     * .
     */
    private String conceptAbbrevName;

    /**
     * \brief String description of a question, where available.
     */
    private String description;

    /**
     * \brief The data type of the answer, as described by the source system (String, number, categorical, etc)
     * .
     */
    private String internalValueType;

    private FormQuestion(FormQuestion other) {
        super(other);
        this.formQuestionId = other.formQuestionId;
        this.conceptName = other.conceptName;
        this.internalId = other.internalId;
        this.conceptAbbrevName = other.conceptAbbrevName;
        this.description = other.description;
        this.internalValueType = other.internalValueType;
    }

    public FormQuestion() {
    }

    /**
     * \brief Create a new FormQuestion with the given values.
     * @param temporalFrom   The validity period of this FormQuestion.
     * @param formQuestionId The internal identifier of this FormQuestion.
     */
    public FormQuestion(TemporalFrom temporalFrom, String formQuestionId) {
        setTemporalFrom(temporalFrom);
        this.internalId = formQuestionId;
    }

    @Override
    public FormQuestion copy() {
        return new FormQuestion(this);
    }

    /**
     * @param validUntil  the event time that invalidated the current state
     * @param storedUntil the time that star started processing the message that invalidated the current state
     * @return A new audit entity with the current state of the object.
     */
    @Override
    public FormQuestionAudit createAuditEntity(Instant validUntil, Instant storedUntil) {
        return new FormQuestionAudit(this, validUntil, storedUntil);
    }
}
