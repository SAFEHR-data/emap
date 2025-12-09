package uk.ac.ucl.rits.inform.interchange.location;


import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import uk.ac.ucl.rits.inform.interchange.EmapOperationMessage;
import uk.ac.ucl.rits.inform.interchange.EmapOperationMessageProcessingException;
import uk.ac.ucl.rits.inform.interchange.EmapOperationMessageProcessor;
import uk.ac.ucl.rits.inform.interchange.EpicRecordStatus;

import java.time.Instant;

/**
 * Department metadata from EPIC.
 * @author Stef Piatek
 */
@Data
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, property = "@class")
public class DepartmentMetadata extends EmapOperationMessage implements MinimalDepartment {
    /**
     * The HL7 identifier for the department, acts as a unique identifier.
     */
    private String departmentHl7;
    /**
     * The department record status.
     * See {@link EpicRecordStatus} for values
     */
    private EpicRecordStatus departmentRecordStatus;
    private Instant departmentContactDate;
    private String departmentName;
    private Long departmentId;
    /**
     * In order to update the department speciality correctly we need to date it
     * changed (i.e. the auditDate).  Department contact date is only used when the
     * previous department speciality does not line up with what is currently in the
     * EMAP database.
     */
    private String departmentSpeciality;
    private Instant specialityUpdate;
    private String previousDepartmentSpeciality;

    /**
     * Variables added to make location processing more robust.
     */
    private String departmentType;
    private Boolean isWardOrFlowArea;
    private Boolean isCoreInpatientArea;
    private String siteLocation;

    /**
     * Not used in processing, but useful to debugging ordering.
     */
    private Instant combinedUpdate;

    @Override
    public void processMessage(EmapOperationMessageProcessor processor) throws EmapOperationMessageProcessingException {
        processor.processMessage(this);
    }
}
