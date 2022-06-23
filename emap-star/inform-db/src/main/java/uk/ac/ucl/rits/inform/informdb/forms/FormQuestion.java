package uk.ac.ucl.rits.inform.informdb.forms;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import uk.ac.ucl.rits.inform.informdb.TemporalCore;
import uk.ac.ucl.rits.inform.informdb.annotation.AuditTable;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import java.time.Instant;

/**
 * Represents the Question in a form (basically the metadata for a form answer).
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
    @GeneratedValue(strategy = GenerationType.AUTO)
    private Long formQuestionId;

    /**
     * \brief String identifier from Epic for a Smart Data Element.
     * Depending on the source of this question, it may not have such an identifier. But for an SDE it will.
     * HLV 40.
     */
    private String smartDataElementIdString;

    /**
     * \brief Smart Data Element name (ie. the "Question")
     * HLX .2
     */
    private String formQuestionConceptName;

    /**
     * \brief Smart Data Element abbreviated name (ie. the "Question")
     * HLX 50
     */
    private String formQuestionConceptAbbrevName;

    /**
     * \brief String description of a question, where available
     */
    private String formQuestionDescription;

    /**
     * \brief Type of answer expected (String, number, etc)
     * Do we store this same value (an instance of ZC_DATA_TYPE) anywhere else in Star?
     * HLX 60
     */
    private String formQuestionValueType;

    /**
     * \brief Categorical string value of the "type" of SDE this is.
     * Ie. is it related to an order, an encounter, a note, etc.
     * Warning: Unknown if this is the same for every instance of the same SDE.
     * If not, this field will have to be moved to FormAnswer.
     * (Convert to enum if we can be sure of possible values?)
     */
    private String smartDataElementContext;

    private FormQuestion(FormQuestion other) {
        super(other);
        this.formQuestionId = other.formQuestionId;
        this.formQuestionConceptName = other.formQuestionConceptName;
        this.smartDataElementIdString = other.smartDataElementIdString;
        this.formQuestionConceptAbbrevName = other.formQuestionConceptAbbrevName;
        this.formQuestionDescription = other.formQuestionDescription;
        this.formQuestionValueType = other.formQuestionValueType;
        this.smartDataElementContext = other.smartDataElementContext;
    }

    public FormQuestion() {
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
