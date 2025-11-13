package uk.ac.ucl.rits.inform.informdb.forms;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.hibernate.annotations.Check;
import uk.ac.ucl.rits.inform.informdb.TemporalCore;
import uk.ac.ucl.rits.inform.informdb.TemporalFrom;
import uk.ac.ucl.rits.inform.informdb.annotation.AuditTable;
import uk.ac.ucl.rits.inform.informdb.identity.HospitalVisit;
import uk.ac.ucl.rits.inform.informdb.identity.Mrn;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.OneToMany;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * \brief A filled out Form. Basically a grouping of rows of FormAnswer.
 */
@Entity
@Data
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
@AuditTable
@Check(constraints = " hospital_visit_id is not null "
        + " OR mrn_id is not null ")
public class Form extends TemporalCore<Form, FormAudit> {
    /**
     * \brief Unique identifier in EMAP for this instance of a Form.
     */
    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    private Long formId;

    /* There is no concept of an instance ID here
     * so there is no formAnswerSourceId here.
     */

    /**
     * \brief The form definition of this form instance.
     */
    @ManyToOne
    @JoinColumn(name = "formDefinitionId")
    private FormDefinition formDefinitionId;

    /**
     * \brief The Mrn this form relates to, or null if it doesn't relate to one.
     */
    @ManyToOne
    @JoinColumn(name = "mrnId")
    private Mrn mrnId;

    /**
     * \brief The hospital visit this form relates to, or null if it doesn't relate to one.
     */
    @ManyToOne
    @JoinColumn(name = "hospitalVisitId")
    private HospitalVisit hospitalVisitId;

    /**
     * \brief A unique ID for this form that can be used to track back to the source system and to look up
     * existing items in Star.
     */
    @Column(nullable = false, unique = true)
    private String internalId;

    /**
     * We don't currently bring in Notes, so there is no other table to join to.
     * If we do in future, this field would be migrated to a foreign key field.
     * <p>
     * \brief NOTE ID if this Form is attached to a note, otherwise null.
     */
    private String noteId;

    /**
     * \brief datetime the form was first filed.
     */
    private Instant firstFiledDatetime;

    @ToString.Exclude
    @OneToMany(mappedBy = "formId", fetch = FetchType.EAGER)
    private List<FormAnswer> formAnswers = new ArrayList<>();

    /**
     * Add the given FormAnswer to this instance of a Form.
     * @param formAnswer the formAnswer to add
     */
    public void addFormAnswer(FormAnswer formAnswer) {
        formAnswers.add(formAnswer);
        formAnswer.setFormId(this);
    }

    public Form() {}

    /**
     * Create a new Form.
     * @param temporalFrom   temporal parameters of the form
     * @param formDefinition specify the type of form of which this is an instance
     * @param hospitalVisit  the hospital visit this form is connected to
     * @param internalId     the form identifier from the source system
     * @param firstFiledDatetime instant for when this form was first filed
     */
    public Form(TemporalFrom temporalFrom, FormDefinition formDefinition, HospitalVisit hospitalVisit,
                String internalId, Instant firstFiledDatetime) {
        setTemporalFrom(temporalFrom);
        this.formDefinitionId = formDefinition;
        this.hospitalVisitId = hospitalVisit;
        this.internalId = internalId;
        this.firstFiledDatetime = firstFiledDatetime;
    }

    private Form(Form other) {
        super(other);
        this.formId = other.formId;
        this.formDefinitionId = other.formDefinitionId;
        this.mrnId = other.mrnId;
        this.hospitalVisitId = other.hospitalVisitId;
        this.internalId = other.internalId;
        this.noteId = other.noteId;
        this.firstFiledDatetime = other.firstFiledDatetime;
    }

    @Override
    public Form copy() {
        return new Form(this);
    }

    /**
     * @param validUntil  the event time that invalidated the current state
     * @param storedUntil the time that star started processing the message that invalidated the current state
     * @return A new audit entity with the current state of the object.
     */
    @Override
    public FormAudit createAuditEntity(Instant validUntil, Instant storedUntil) {
        return new FormAudit(this, validUntil, storedUntil);
    }
}
