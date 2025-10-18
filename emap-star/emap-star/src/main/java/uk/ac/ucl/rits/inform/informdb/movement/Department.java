package uk.ac.ucl.rits.inform.informdb.movement;

import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;
import java.io.Serializable;

/**
 * \brief Represents a department in the hospital.
 */
@SuppressWarnings("serial")
@Entity
@Data
@Table
@NoArgsConstructor
public class Department implements Serializable {

    /**
     * \brief Unique identifier in EMAP for this department record.
     * <p>
     * This is the primary key for the department table.
     */
    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    private Long departmentId;

    /**
     * \brief Text name used by HL7 for this department.
     */
    private String hl7String;

    @Column(nullable = false)
    private Long internalId;

    /**
     * \brief Name of this department.
     */
    private String name;

    /**
     * \brief Indicates if this department is a ward or flow area.
     */
    private boolean isWardOrFlowArea;

    /**
     *  \brief Indicates if this department is a core inpatient area.
     */
    private boolean isCoreInpatientArea;



    /**
     * Create minimal department.
     * @param internalId ID of the department in EPIC.
     * @param isWardOrFlowArea whether department is ward or flow area
     * @param isCoreInpatientArea whether department is core inpatient area
     */
    public Department(Long internalId, boolean isWardOrFlowArea, boolean isCoreInpatientArea) {
        this.internalId = internalId;
        this.isWardOrFlowArea = isWardOrFlowArea;
        this.isCoreInpatientArea = isCoreInpatientArea;
    }

    /**
     * Create minimal department.
     * @param internalId ID of the department in EPIC.
     */
    public Department(Long internalId) {
        this(internalId, false, true);
    }
}
