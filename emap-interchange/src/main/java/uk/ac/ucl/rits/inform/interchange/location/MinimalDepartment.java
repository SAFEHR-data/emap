package uk.ac.ucl.rits.inform.interchange.location;

public interface MinimalDepartment {
    Long getDepartmentId();
    boolean isWardOrFlowArea();
    boolean isCoreInpatientArea();
}
