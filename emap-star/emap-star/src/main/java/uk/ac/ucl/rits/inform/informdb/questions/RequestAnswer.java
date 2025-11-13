package uk.ac.ucl.rits.inform.informdb.questions;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import uk.ac.ucl.rits.inform.informdb.TemporalCore;
import uk.ac.ucl.rits.inform.informdb.annotation.AuditTable;

import javax.persistence.Column;
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

/**
 * \brief Answers to questions listed in Question table.
 *
 * Several data types, such as e.g. lab samples and consultation requests, hold questions. As these data types are
 * progressed within the hospital, e.g. lab samples are analysed, these questions are likely to be answered. The
 * answers to questions are held in EMAP as RequestAnswers and link to both the question that triggered the answer and
 * the data type that the questions belong to.
 * @author Anika Cawthorn
*/
@Entity
@Data
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
@AuditTable
@Table(indexes = {@Index(name = "idxParentId", columnList = "parentId")})
public class RequestAnswer extends TemporalCore<RequestAnswer, RequestAnswerAudit> {

    /**
     * \brief Unique identifier in EMAP for this requestAnswerId record.
     *
     * This is the primary key for the requestAnswer table.
     */
    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    private long requestAnswerId;

    /**
     * \brief Text content of the requestAnswer.
     */
    @Lob
    private String answer;

    /**
     * \brief Identifier for the Question associated with this record.
     *
     * This is a foreign key that joins the requestAnswer table to the Question table.
     */
    @ManyToOne
    @JoinColumn(name = "questionId", nullable = false)
    private Question questionId;

    /**
     * \brief Parent table that should be included as a part of the join.
     *
     * E.g. The value PATIENT_CONDITION should be a part of the join criteria for PatientCondition questions
     * Because there are multiple data types that trigger questions and we hold them all together in one table,
     * this column is an identifier to specify the data type that particular question corresponds to
     */
    @Column(nullable = false)
    private String parentTable;

    /**
     * \brief Identifier for the parentTable associated with this record.
     *
     * This is a foreign key that joins the appropriate table to the Question table.
     */
    @Column(nullable = false)
    private long parentId;

    public RequestAnswer() {}

    /**
     * Minimal request answer constructor that requires that the answer and the timestamps for when EMAP
     * started processing this data type and from which time point the question information is valid from.
     * @param questionId    Question this answer relates to
     * @param answer        The actual question string linked to a data type
     * @param parentTable   Data type that triggered question-answer pair creation, e.g. lab sample or consultation
     *                      request
     * @param parentId      Entity (e.g. lab sample or consultation request) that triggered the question-answer pair
     *                      to be created
     * @param validFrom     Timestamp from which information valid from
     * @param storedFrom    When EMAP started processing this data type
     */
    public RequestAnswer(Question questionId, String answer, String parentTable, long parentId, Instant validFrom,
                         Instant storedFrom) {
        this.answer = answer;
        this.questionId = questionId;
        this.parentTable = parentTable;
        this.parentId = parentId;
        setStoredFrom(storedFrom);
        setValidFrom(validFrom);
    }

    /**
     * Copy values of RequestAnswer to new instance thereof.
     * @param other Instance of RequestAnswer from which values are copied to newly created instance.
     */
    public RequestAnswer(RequestAnswer other) {
        super(other);
        this.requestAnswerId = other.requestAnswerId;
        this.answer = other.answer;
        this.questionId = other.questionId;
        this.parentTable = other.parentTable;
        this.parentId = other.parentId;
    }

    @Override
    public RequestAnswer copy() {
        return new RequestAnswer(this);
    }

    @Override
    public RequestAnswerAudit createAuditEntity(Instant validUntil, Instant storedUntil) {
        return new RequestAnswerAudit(this, validUntil, storedUntil);
    }
}
