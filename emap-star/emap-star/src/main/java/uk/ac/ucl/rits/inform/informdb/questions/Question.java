package uk.ac.ucl.rits.inform.informdb.questions;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Lob;
import java.time.Instant;

/**
 * \brief Questions that can be attached to several data types, such as lab samples or consultation requests.
 *
 * Independent of
 * which type of question, these are all held together in one table and reference back to the entity they relate to. It
 * is to be noted here that questions at the moment are only cumulative and cannot be deleted.
 * @author Stef Piatek
 * @author Anika Cawthorn
 */
@Entity
@Data
@ToString(callSuper = true)
@NoArgsConstructor
public class Question {

    /**
     * \brief Unique identifier in EMAP for this question record.
     *
     * This is the primary key for the question table.
     */
    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    private long questionId;

    /**
     * \brief Text content of the question.
     */
    @Lob
    @Column(nullable = false)
    private String question;

    /**
     * \brief Date and time from which this question is stored.
     */
    @Column(nullable = false)
    private Instant storedFrom;

    /**
     * \brief Date and time from which this question is valid.
     */
    @Column(nullable = false)
    private Instant validFrom;

    /**
     * Minimal question constructor that requires the question as such and the timestamps for when EMAP
     * started processing this data type and from which time point the question information is valid from.
     * @param question      The actual question string linked to a data type
     * @param validFrom     Timestamp from which information valid from
     * @param storedFrom    When EMAP started processing this data type
     */
    public Question(String question, Instant validFrom, Instant storedFrom) {
        this.question = question;
        this.storedFrom = storedFrom;
        this.validFrom = validFrom;
    }
}
