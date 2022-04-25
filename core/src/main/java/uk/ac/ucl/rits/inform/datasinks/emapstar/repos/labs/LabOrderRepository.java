package uk.ac.ucl.rits.inform.datasinks.emapstar.repos.labs;

import org.springframework.data.repository.CrudRepository;
import uk.ac.ucl.rits.inform.informdb.identity.HospitalVisit;
import uk.ac.ucl.rits.inform.informdb.labs.LabBattery;
import uk.ac.ucl.rits.inform.informdb.labs.LabOrder;
import uk.ac.ucl.rits.inform.informdb.labs.LabSample;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

/**
 * Lab order repository.
 * @author Stef Piatek
 */
public interface LabOrderRepository extends CrudRepository<LabOrder, Long> {
    Optional<LabOrder> findByLabBatteryIdAndLabSampleId(LabBattery battery, LabSample number);

    Optional<LabOrder> findByLabBatteryIdAndLabSampleIdAndValidFromBefore(LabBattery battery, LabSample number, Instant validFrom);

    /**
     * for testing.
     * @param labNumber laboratory lab number
     * @return LabOrder
     */
    Optional<LabOrder> findByLabSampleIdExternalLabNumber(String labNumber);

    /**
     * for testing.
     * @param battery   code
     * @param labSample labSample
     * @return optional lab order
     */
    Optional<LabOrder> findByLabBatteryIdBatteryCodeAndLabSampleId(String battery, LabSample labSample);


    List<LabOrder> findByHospitalVisitId(HospitalVisit hospitalVisit);

}
