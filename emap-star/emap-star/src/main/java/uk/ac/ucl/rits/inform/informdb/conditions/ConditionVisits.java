package uk.ac.ucl.rits.inform.informdb.conditions;
import uk.ac.ucl.rits.inform.informdb.identity.HospitalVisit;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.Index;
import javax.persistence.Table;


/**
 * Linker table between the PatientCondition and HospitalVisit tables to efficiently model the many-many relationship
 * between the two.
 *
 * @author Tom Young
 */
@Entity
@Data
@EqualsAndHashCode
@NoArgsConstructor
@ToString(callSuper = true)
@Table(indexes = {@Index(name = "cv_hospital_visit_id", columnList = "hospitalVisitId"),
        @Index(name = "cv_condition_visits_id", columnList = "conditionVisitsId")})
public class ConditionVisits {

    /**
     * \brief Unique identifier in EMAP for this condition visit record.
     *
     * This is the primary key for the ConditionVisits table.
     */
    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "emap_id_sequence")
    private Long conditionVisitsId;

    /**
     * \brief Identifier for the HospitalVisit associated with this record.
     *
     * This is a foreign key that joins to the HospitalVisit table.
     */
    @ManyToOne
    @JoinColumn(name = "hospitalVisitId", nullable = false)
    private HospitalVisit hospitalVisitId;


    /**
     * \brief Identifier for the PatientCondition associated with this record.
     *
     * This is a foreign key that joins to the PatientCondition table.
     */
    @ManyToOne
    @JoinColumn(name = "patientConditionId", nullable = false)
    private PatientCondition patientConditionId;

    /**
     * Minimal information constructor.
     * @param condition ID for patient condition
     * @param visit ID for hospital visit
     */
    public ConditionVisits(PatientCondition condition, HospitalVisit visit) {
        this.patientConditionId = condition;
        this.hospitalVisitId = visit;
    }
}
