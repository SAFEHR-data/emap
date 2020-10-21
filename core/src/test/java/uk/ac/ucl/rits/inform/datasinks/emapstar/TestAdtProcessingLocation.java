package uk.ac.ucl.rits.inform.datasinks.emapstar;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.jdbc.Sql;
import uk.ac.ucl.rits.inform.datasinks.emapstar.repos.AuditLocationVisitRepository;
import uk.ac.ucl.rits.inform.datasinks.emapstar.repos.HospitalVisitRepository;
import uk.ac.ucl.rits.inform.datasinks.emapstar.repos.LocationRepository;
import uk.ac.ucl.rits.inform.datasinks.emapstar.repos.LocationVisitRepository;
import uk.ac.ucl.rits.inform.informdb.movement.AuditLocationVisit;
import uk.ac.ucl.rits.inform.informdb.movement.LocationVisit;
import uk.ac.ucl.rits.inform.interchange.EmapOperationMessageProcessingException;
import uk.ac.ucl.rits.inform.interchange.adt.AdmitPatient;
import uk.ac.ucl.rits.inform.interchange.adt.TransferPatient;

class TestAdtProcessingLocation extends MessageProcessingBase {
    @Autowired
    private HospitalVisitRepository hospitalVisitRepository;
    @Autowired
    private LocationRepository locationRepository;
    @Autowired
    private LocationVisitRepository locationVisitRepository;
    @Autowired
    private AuditLocationVisitRepository auditLocationVisitRepository;

    private final String originalLocation = "T42E^T42E BY03^BY03-17";
    private final long defaultHospitalVisitId = 4001;

    /**
     * No locations or location-visit in database.
     * Should create a new location and location-visit, but no audit location visit
     * @throws EmapOperationMessageProcessingException shouldn't happen
     */
    @Test
    void testCreateNewLocationVisit() throws EmapOperationMessageProcessingException {
        AdmitPatient msg = messageFactory.getAdtMessage("generic/A01.yaml");
        dbOps.processMessage(msg);

        Assertions.assertEquals(1L, getAllEntities(locationRepository).size());
        Assertions.assertEquals(1L, getAllEntities(locationVisitRepository).size());
        Assertions.assertEquals(0L, getAllEntities(auditLocationVisitRepository).size());
    }


    /**
     * Visit and location visit already exist in the database, new location given.
     * Should discharge the original location visit, audit log the original state and create a new location.
     * @throws EmapOperationMessageProcessingException shouldn't happen
     */
    @Test
    @Sql(value = "/populate_db.sql")
    void testMoveCurrentVisitLocation() throws EmapOperationMessageProcessingException {
        TransferPatient msg = messageFactory.getAdtMessage("generic/A02.yaml");
        dbOps.processMessage(msg);

        // original location visit is discharged
        LocationVisit dischargedVisit = locationVisitRepository.findByLocationIdLocationString(originalLocation).orElseThrow(NullPointerException::new);
        Assertions.assertNotNull(dischargedVisit.getDischargeTime());

        // current location visit is different
        LocationVisit currentVisit = locationVisitRepository
                .findByDischargeTimeIsNullAndHospitalVisitIdHospitalVisitId(defaultHospitalVisitId)
                .orElseThrow(NullPointerException::new);
        Assertions.assertNotEquals(originalLocation, currentVisit.getLocation().getLocationString());

        // audit row for location when it had no discharge time
        AuditLocationVisit audit = auditLocationVisitRepository.findByLocationIdLocationString(originalLocation).orElseThrow(NullPointerException::new);
        Assertions.assertNull(audit.getDischargeTime());
    }

}
