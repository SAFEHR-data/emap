package uk.ac.ucl.rits.inform.datasinks.emapstar.adt;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import uk.ac.ucl.rits.inform.datasinks.emapstar.MessageProcessingBase;
import uk.ac.ucl.rits.inform.datasinks.emapstar.repos.CoreDemographicRepository;
import uk.ac.ucl.rits.inform.datasinks.emapstar.repos.HospitalVisitRepository;
import uk.ac.ucl.rits.inform.datasinks.emapstar.repos.MrnRepository;
import uk.ac.ucl.rits.inform.datasinks.emapstar.repos.PlannedMovementRepository;
import uk.ac.ucl.rits.inform.informdb.movement.PlannedMovement;
import uk.ac.ucl.rits.inform.interchange.InterchangeValue;
import uk.ac.ucl.rits.inform.interchange.adt.PendingTransfer;
import uk.ac.ucl.rits.inform.interchange.adt.UpdatePatientInfo;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.NoSuchElementException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * A patient info update (ADT^A08) should feed the same ADT-triggered hospital service fallback as Z99,
 * per <a href="https://github.com/SAFEHR-data/emap/issues/166">#166</a>. Also covers the issue's explicit
 * deduplication requirement: repeated A08s carrying the same, already-recorded hospital service must not
 * flood planned_movement with duplicate rows.
 */
class TestUpdatePatientInfoHospitalServiceFallback extends MessageProcessingBase {
    @Autowired
    private MrnRepository mrnRepository;
    @Autowired
    private CoreDemographicRepository coreDemographicRepository;
    @Autowired
    private HospitalVisitRepository hospitalVisitRepository;
    @Autowired
    private PlannedMovementRepository plannedMovementRepository;

    private UpdatePatientInfo updatePatientInfo;
    private PendingTransfer pendingTransfer;
    private PendingTransfer pendingTransferLater;
    private PendingTransfer pendingTransferAfter;

    private static final String VISIT_NUMBER = "123412341234";
    private static final String LOCATION_STRING = "1020100166^SDEC BY02^11 SDEC";
    private static final Instant UPDATE_EVENT_TIME = Instant.parse("2022-04-22T00:00:00Z");

    private PlannedMovement getPlannedMovementOrThrow(String visitNumber, String location) {
        return plannedMovementRepository
                .findByHospitalVisitIdEncounterAndLocationIdLocationString(visitNumber, location).orElseThrow();
    }

    @BeforeEach
    void setup() throws IOException {
        updatePatientInfo = messageFactory.getAdtMessage("generic/A08_v1.yaml");
        updatePatientInfo.setFullLocationString(InterchangeValue.buildFromHl7(LOCATION_STRING));
        updatePatientInfo.setEventOccurredDateTime(UPDATE_EVENT_TIME);

        pendingTransfer = messageFactory.getAdtMessage("pending/A15.yaml");
        pendingTransferLater = messageFactory.getAdtMessage("pending/A15.yaml");
        pendingTransferAfter = messageFactory.getAdtMessage("pending/A15.yaml");

        Instant laterTime = pendingTransferLater.getEventOccurredDateTime().plus(1, ChronoUnit.MINUTES);
        pendingTransferLater.setEventOccurredDateTime(laterTime);

        Instant afterTime = pendingTransferAfter.getEventOccurredDateTime().plus(1, ChronoUnit.HOURS);
        pendingTransferAfter.setEventOccurredDateTime(afterTime);
    }

    /**
     * Given that no entities exist in the database
     * When an update is processed
     * Mrn and core demographics entities should be created (UpdatePatientInfo does not create its own
     * hospital visit), and no planned movement fallback row should be created as there is nothing to
     * match against.
     */
    @Test
    void testUpdateCreatesOtherEntitiesNoFallback() throws Exception {
        dbOps.processMessage(updatePatientInfo);

        assertEquals(1, mrnRepository.count());
        assertEquals(1, coreDemographicRepository.count());
        assertEquals(0, hospitalVisitRepository.count());

        assertThrows(NoSuchElementException.class, () -> getPlannedMovementOrThrow(VISIT_NUMBER, LOCATION_STRING));
    }

    /**
     * If more than one pending transfer exists find the most recent one and if the update
     * has a different hospital service insert the edit into the planned movement table.
     */
    @Test
    void testUpdateInsertsEditIfHospitalServicesAreDifferent() throws Exception {
        dbOps.processMessage(pendingTransfer);
        dbOps.processMessage(pendingTransferLater);
        dbOps.processMessage(pendingTransferAfter);
        dbOps.processMessage(updatePatientInfo);

        List<PlannedMovement> movements = plannedMovementRepository.findAllByHospitalVisitIdEncounter(VISIT_NUMBER);
        assertEquals(4, movements.size());
        assertEquals("EDIT/HOSPITAL_SERVICE_CHANGE", movements.get(3).getEventType());
        assertEquals(Instant.parse("2022-04-22T00:00:00Z"), movements.get(3).getEventDatetime());
    }

    /**
     * Find the most recent matching planned movement, but don't add to the table
     * if the update has the same hospital service as it.
     */
    @Test
    void testUpdateDoesNotInsertIfHospitalServicesAreTheSame() throws Exception {
        dbOps.processMessage(pendingTransfer);
        updatePatientInfo.setHospitalService(pendingTransfer.getHospitalService());
        dbOps.processMessage(updatePatientInfo);

        List<PlannedMovement> movements = plannedMovementRepository.findAllByHospitalVisitIdEncounter(VISIT_NUMBER);
        assertEquals(1, movements.size());
        assertEquals("TRANSFER", movements.get(0).getEventType());
    }

    /**
     * If pending transfers only exist after the update event, don't add the fallback edit.
     */
    @Test
    void testUpdateDoesNotInsertIfTransfersAreAfter() throws Exception {
        dbOps.processMessage(pendingTransferAfter);
        dbOps.processMessage(updatePatientInfo);

        List<PlannedMovement> movements = plannedMovementRepository.findAllByHospitalVisitIdEncounter(VISIT_NUMBER);
        assertEquals(1, movements.size());
        assertEquals("TRANSFER", movements.get(0).getEventType());
    }

    /**
     * Real A08 traffic reconfirms the same subspeciality repeatedly for hours after admission.
     * A second A08 carrying the same hospital service as the fallback row just inserted by the
     * first one must not insert a duplicate row.
     */
    @Test
    void testRepeatedUpdatesWithSameHospitalServiceAreDeduplicated() throws Exception {
        dbOps.processMessage(pendingTransfer);
        dbOps.processMessage(updatePatientInfo);

        UpdatePatientInfo secondUpdate = messageFactory.getAdtMessage("generic/A08_v1.yaml");
        secondUpdate.setFullLocationString(InterchangeValue.buildFromHl7(LOCATION_STRING));
        secondUpdate.setEventOccurredDateTime(UPDATE_EVENT_TIME.plus(1, ChronoUnit.HOURS));
        secondUpdate.setHospitalService(updatePatientInfo.getHospitalService());
        dbOps.processMessage(secondUpdate);

        List<PlannedMovement> movements = plannedMovementRepository.findAllByHospitalVisitIdEncounter(VISIT_NUMBER);
        assertEquals(2, movements.size());
        assertEquals("EDIT/HOSPITAL_SERVICE_CHANGE", movements.get(1).getEventType());
    }
}
