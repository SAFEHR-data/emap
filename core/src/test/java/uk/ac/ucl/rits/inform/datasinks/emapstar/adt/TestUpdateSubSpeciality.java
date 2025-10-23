package uk.ac.ucl.rits.inform.datasinks.emapstar.adt;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import uk.ac.ucl.rits.inform.datasinks.emapstar.MessageProcessingBase;
import uk.ac.ucl.rits.inform.datasinks.emapstar.repos.CoreDemographicRepository;
import uk.ac.ucl.rits.inform.datasinks.emapstar.repos.HospitalVisitRepository;
import uk.ac.ucl.rits.inform.datasinks.emapstar.repos.MrnRepository;
import uk.ac.ucl.rits.inform.datasinks.emapstar.repos.PlannedMovementRepository;
import uk.ac.ucl.rits.inform.informdb.movement.PlannedMovement;
import uk.ac.ucl.rits.inform.interchange.adt.PendingTransfer;
import uk.ac.ucl.rits.inform.interchange.adt.UpdateSubSpeciality;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.NoSuchElementException;


import static org.junit.jupiter.api.Assertions.*;

class TestUpdateSubSpeciality extends MessageProcessingBase {
    private static final Logger logger = LoggerFactory.getLogger(TestPendingAdt.class);
    @Autowired
    private MrnRepository mrnRepository;
    @Autowired
    private CoreDemographicRepository coreDemographicRepository;
    @Autowired
    private HospitalVisitRepository hospitalVisitRepository;
    @Autowired
    private PlannedMovementRepository plannedMovementRepository;

    // end to end messages
    private UpdateSubSpeciality updateSubSpeciality;
    private PendingTransfer pendingTransfer;
    private PendingTransfer pendingTransferLater;
    private PendingTransfer pendingTransferAfter;

    private static final String VISIT_NUMBER = "123412341234";
    private static final String LOCATION_STRING = "1020100166^SDEC BY02^11 SDEC";


    private PlannedMovement getPlannedMovementOrThrow(String visitNumber, String location) {
        return plannedMovementRepository
                .findByHospitalVisitIdEncounterAndLocationIdLocationString(visitNumber, location).orElseThrow();
    }

    @BeforeEach
    void setup() throws IOException {
        updateSubSpeciality = messageFactory.getAdtMessage("Location/Moves/09_Z99.yaml");

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
     * When a Z99 Message is created
     * Mrn, core demographics and hospital visit entities should be created.
     * A planned movement should not be created as there are no matching planned moves in the
     * planned movement table
     * @throws Exception shouldn't happen
     */
    @Test
    void testUpdateCreatesOtherEntities() throws Exception {
        dbOps.processMessage(updateSubSpeciality);

        assertEquals(1, mrnRepository.count());
        assertEquals(1, coreDemographicRepository.count());
        assertEquals(1, hospitalVisitRepository.count());

        assertThrows(NoSuchElementException.class, () -> getPlannedMovementOrThrow(VISIT_NUMBER, LOCATION_STRING));
    }

    /**
     * If more than one pending transfer exists find the most recent one and if it
     * has a different hospital service insert the edit into the planned movement table.
     */
    @Test
    void testEditMessageInsertedIfHospitalServicesAreDifferent() throws Exception {

        dbOps.processMessage(pendingTransfer);
        dbOps.processMessage(pendingTransferLater);
        dbOps.processMessage(pendingTransferAfter);
        dbOps.processMessage(updateSubSpeciality);

        // and entry should have been added to the planned movement table with the correct matched planned movement id
        List<PlannedMovement> movements = plannedMovementRepository.findAllByHospitalVisitIdEncounter(VISIT_NUMBER);
        assertEquals(4, movements.size());
        assertEquals("EDIT/HOSPITAL_SERVICE_CHANGE", movements.get(3).getEventType());
        assertEquals(7, movements.get(3).getMatchedMovementId());
        assertEquals(Instant.parse("2022-04-22T00:00:00Z"), movements.get(3).getEventDatetime());
    }

    /**
    * Find the most recent one, but don't add to table if it has the same hospital service as the edit message
    */
    @Test
    void testEditMessageNotInsertedIfHospitalServicesAreTheSame() throws Exception {
        dbOps.processMessage(pendingTransfer);
        updateSubSpeciality.setHospitalService(pendingTransfer.getHospitalService());
        dbOps.processMessage(updateSubSpeciality);

        // and entry should have been added to the planned movement table with the correct matched planned movement id
        List<PlannedMovement> movements = plannedMovementRepository.findAllByHospitalVisitIdEncounter(VISIT_NUMBER);
        assertEquals(1, movements.size());
        assertEquals("TRANSFER", movements.get(0).getEventType());
    }

    /**
     * If pending transfers only exist after the edit event, don't add edit message.
     */
    @Test
    void testEditMessageNotInsertedIfTransfersAreAfter() throws Exception {
        dbOps.processMessage(pendingTransferAfter);
        dbOps.processMessage(updateSubSpeciality);

        assertEquals(1, mrnRepository.count());
        assertEquals(1, coreDemographicRepository.count());
        assertEquals(1, hospitalVisitRepository.count());

        // one entry should have been added to the planned movement table with the correct matched planned movement id
        List<PlannedMovement> movements = plannedMovementRepository.findAllByHospitalVisitIdEncounter(VISIT_NUMBER);
        assertEquals(1, movements.size());
        assertEquals("TRANSFER", movements.get(0).getEventType());
        //assertThrows(NoSuchElementException.class, () -> getPlannedMovementOrThrow(VISIT_NUMBER, LOCATION_STRING));
    }
}
