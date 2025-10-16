package uk.ac.ucl.rits.inform.interchange.adt;

import uk.ac.ucl.rits.inform.interchange.InterchangeValue;


/**
 * Ensuring admission date time is used in a class.
 * <p>
 * Used in specific circumstances because where the sub speciality is changed without a movement
 * via a ADT Z99 message
 */
public interface HospitalService {
    InterchangeValue<String> getHospitalService();

    void setHospitalService(InterchangeValue<String> hospitalService);
}
