package uk.ac.ucl.rits.inform.interchange.adt;

import uk.ac.ucl.rits.inform.interchange.InterchangeValue;

/**
 * Shared fields for all Pending Event interchange message types.
 * @author Stef Piatek
 */
public interface PendingEvent {
    /**
     * @return Type of pending event
     */
    PendingType getPendingEventType();

    /**
     * @return Pending transfer location if known
     */
    InterchangeValue<String> getPendingDestination();

    /**
     * @param location Concatenated location string
     */
    void setPendingDestination(InterchangeValue<String> location);


    /**
     * @return Hospital service from PV1-10.
     */
    InterchangeValue<String> getHospitalService();

    /**
     * @param hospitalService
     */
    void  setHospitalService(InterchangeValue<String> hospitalService);

}
