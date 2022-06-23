package uk.ac.ucl.rits.inform.informdb.forms;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.hibernate.annotations.Check;
import uk.ac.ucl.rits.inform.informdb.TemporalCore;
import uk.ac.ucl.rits.inform.informdb.annotation.AuditTable;
import uk.ac.ucl.rits.inform.informdb.identity.HospitalVisit;
import uk.ac.ucl.rits.inform.informdb.identity.Mrn;
import uk.ac.ucl.rits.inform.informdb.labs.LabOrder;

import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
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
 * A filled out Form. Eg. an Epic SmartForm. Basically a grouping of rows of FormAnswer.
 */
@Entity
@Data
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
@AuditTable
@Check(constraints =
        " hospital_visit_id is not null " +
                " OR lab_order_id is not null " +
                " OR mrn_id is not null ")
public class Form extends TemporalCore<Form, FormAudit> {
    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    private Long formId;

    /**
     * \brief The form definition of this form instance.
     */
    @ManyToOne
    @JoinColumn(name = "formDefinitionId")
    private FormDefinition formDefinitionId;

    /**
     * \brief The Mrn this SmartForm relates to, or null if it doesn't relate to one.
     */
    @ManyToOne
    @JoinColumn(name = "mrnId")
    private Mrn mrnId;

    /**
     * \brief The hospital visit this SmartForm relates to, or null if it doesn't relate to one.
     */
    @ManyToOne
    @JoinColumn(name = "hospitalVisitId")
    private HospitalVisit hospitalVisitId;

    /**
     * We don't currently bring in Notes, so there is no other table to join to.
     * If we do in future, this field would be migrated to a foreign key field.
     * \brief NOTE ID if this Form is attached to a note, otherwise null.
     */
    private String noteId;

    /**
     * \brief The lab order this SmartForm relates to, or null if it doesn't relate to one.
     * I'm not currently sure if SmartForms can be attached to Lab Orders.
     */
    @ManyToOne
    @JoinColumn(name = "labOrderId")
    private LabOrder labOrderId;

    /**
     * \brief datetime the form was filed.
     * This may be redundant given valid from?
     */
    @Column(columnDefinition = "timestamp with time zone")
    private Instant formFilingDatetime;

    private String formFilingUserId;

    @ToString.Exclude
    @OneToMany(mappedBy = "formId", cascade = CascadeType.ALL)
    private List<FormAnswer> formAnswers = new ArrayList<>();

    public void addFormAnswer(FormAnswer formAnswer) {
        formAnswers.add(formAnswer);
        formAnswer.setFormId(this);
    }

    public Form() {}

    private Form(Form other) {
        super(other);
        this.formId = other.formId;
        this.formDefinitionId = other.formDefinitionId;
        this.mrnId = other.mrnId;
        this.hospitalVisitId = other.hospitalVisitId;
        this.noteId = other.noteId;
        this.labOrderId = other.labOrderId;
        this.formFilingDatetime = other.formFilingDatetime;
        this.formFilingUserId = other.formFilingUserId;
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