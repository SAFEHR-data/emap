package uk.ac.ucl.rits.inform.informdb.questions;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;
import uk.ac.ucl.rits.inform.informdb.TemporalCore;
import uk.ac.ucl.rits.inform.informdb.annotation.AuditTable;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import java.time.Instant;

/**
 * Questions that can be attached to several data types, such as lab samples or consultation requests. Independent of
 * which type of question, these are all held together in one table and reference back to the entity they relate to. It
 * is to be noted here that questions at the moment are only cumulative and cannot be deleted.
 * @author Stef Piatek
 * @author Anika Cawthorn
 */
@Entity
@Data
@ToString(callSuper = true)
@AuditTable
@NoArgsConstructor
public class Question {
    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    private long questionId;

    @Column(columnDefinition = "text", nullable = false)
    private String question;

    @Column(nullable = false)
    private Instant storedFrom;

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
