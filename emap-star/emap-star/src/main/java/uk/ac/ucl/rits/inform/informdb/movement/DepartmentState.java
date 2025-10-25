package uk.ac.ucl.rits.inform.informdb.movement;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;
import uk.ac.ucl.rits.inform.informdb.AuditCore;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.Table;
import java.time.Instant;

/**
 * \brief Represents the state of a given Department.
 */
@SuppressWarnings("serial")
@Entity
@Data
@ToString(callSuper = true)
@Table
@NoArgsConstructor
@EqualsAndHashCode(callSuper = true)
public class DepartmentState extends AuditCore<DepartmentState> {

    /**
     * \brief Unique identifier in EMAP for this departmentState record.
     *
     * This is the primary key for the departmentState table.
     */
    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    private Long departmentStateId;

    /**
     * \brief Identifier for the Department associated with this record.
     *
     * This is a foreign key that joins the departmentState table to the Department table.
     */
    @ManyToOne
    @JoinColumn(name = "departmentId", nullable = false)
    private Department departmentId;

    /**
     * \brief Current status of the Department.
     */
    private String status;

    /**
     * \brief Current speciality of the department.
     */
    private String speciality;

    /**
     * \brief Report Group Nine (departmentType) value associated with this department.
     */
    private String departmentType;

    /**
     * \brief Predicate determining whether the Department is a ward or flow area.
     */
    private Boolean isWardOrFlowArea;

    /**
     * \brief Predicate determining whether the Department is a core inpatient area.
     */
    private Boolean isCoreInpatientArea;

    /**
     * \brief hospital location where this department is situated.
     */
    private String siteLocation; 

    /**
     * Create valid department state.
     * @param department parent department
     * @param status     status of department
     * @param validFrom  time that the message was valid from
     * @param storedFrom time that emap core stared processing the message
     * @param speciality name of the current speciality of this department
     * @param departmentType Report Group Nine value associated with this department
     * @param isWardOrFlowArea is this department a ward or flow area
     * @param isCoreInpatientArea is this department a core inpatient area
     * @param siteLocation hospital location where this department is situated.
     */
    public DepartmentState(Department department, String status,
    String speciality, String departmentType, Boolean isWardOrFlowArea,
    Boolean isCoreInpatientArea, String siteLocation, Instant validFrom, Instant storedFrom) {
        departmentId = department;
        this.status = status;
        this.speciality = speciality;
        this.departmentType = departmentType;
        this.isWardOrFlowArea = isWardOrFlowArea;
        this.isCoreInpatientArea = isCoreInpatientArea;
        this.siteLocation = siteLocation;
        setValidFrom(validFrom);
        setStoredFrom(storedFrom);
    }

    private DepartmentState(DepartmentState other) {
        super(other);
        setValidFrom(other.getValidFrom());
        setStoredFrom(other.getStoredFrom());
        departmentStateId = other.departmentStateId;
        departmentId = other.departmentId;
        status = other.status;
        speciality = other.speciality;
        departmentType = other.departmentType;
        isWardOrFlowArea = other.isWardOrFlowArea;
        isCoreInpatientArea = other.isCoreInpatientArea;
        siteLocation = other.siteLocation;
    }

    @Override
    public DepartmentState copy() {
        return new DepartmentState(this);
    }

    /**
     * @param validUntil  the event time that invalidated the current state
     * @param storedUntil the time that star started processing the message that invalidated the current state
     * @return A new audit entity with the current state of the object.
     */
    @Override
    public DepartmentState createAuditEntity(Instant validUntil, Instant storedUntil) {
        DepartmentState audit = copy();
        audit.setValidUntil(validUntil);
        audit.setStoredUntil(storedUntil);
        return audit;
    }

    /**
     * Checks if this DepartmentState has been superseded by a new entry into the table.
     * @return boolean indicating if this is a previous state of the department.
     */
    public boolean isPrevious() {
        return getValidFrom() != null && getValidUntil() != null && getStoredFrom() != null && getStoredUntil() != null;
    }
    /**
     * Is this the most up to date DepartmentState for a particular department?
     * @return boolean indicating if this is the current departments state.
     */
    public boolean isCurrent() {
        return getValidFrom() != null && getValidUntil() != null && getStoredFrom() != null && getStoredUntil() != null;
    }

}
