package uk.ac.ucl.rits.inform.datasinks.emapstar.adt;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import uk.ac.ucl.rits.inform.datasinks.emapstar.MessageProcessingBase;
import uk.ac.ucl.rits.inform.datasinks.emapstar.repos.PlannedMovementRepository;
import uk.ac.ucl.rits.inform.informdb.movement.PlannedMovement;
import uk.ac.ucl.rits.inform.interchange.InterchangeValue;
import uk.ac.ucl.rits.inform.interchange.adt.AdmitPatient;
import uk.ac.ucl.rits.inform.interchange.adt.CancelAdmitPatient;
import uk.ac.ucl.rits.inform.interchange.adt.PendingTransfer;

import java.io.IOException;
import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Cancelling an admission (ADT^A11) should cancel the matching ADMISSION row created by
 * {@link uk.ac.ucl.rits.inform.datasinks.emapstar.controllers.PendingAdtController#processAdmission},
 * but must never touch a planned_movement row that originated from a real pending transfer/discharge
 * request.
 */
class TestCancelAdmitPatientHospitalServiceFallback extends MessageProcessingBase {
    @Autowired
    private PlannedMovementRepository plannedMovementRepository;

    private PendingTransfer pendingTransfer;
    private AdmitPatient admitPatient;
    private CancelAdmitPatient cancelAdmitPatient;

    private static final String VISIT_NUMBER = "123412341234";
    private static final String LOCATION_STRING = "1020100166^SDEC BY02^11 SDEC";
    private static final Instant ADMISSION_EVENT_TIME = Instant.parse("2022-04-22T00:00:00Z");

    @BeforeEach
    void setup() throws IOException {
        pendingTransfer = messageFactory.getAdtMessage("pending/A15.yaml");

        admitPatient = messageFactory.getAdtMessage("generic/A01.yaml");
        admitPatient.setFullLocationString(InterchangeValue.buildFromHl7(LOCATION_STRING));
        admitPatient.setEventOccurredDateTime(ADMISSION_EVENT_TIME);

        cancelAdmitPatient = messageFactory.getAdtMessage("generic/A11.yaml");
        cancelAdmitPatient.setFullLocationString(InterchangeValue.buildFromHl7(LOCATION_STRING));
        cancelAdmitPatient.setCancelledDateTime(ADMISSION_EVENT_TIME);
    }

    /**
     * If no planned movement exists at all, cancelling the admission is a no-op.
     */
    @Test
    void testNoPlannedMovementsIsNoOp() throws Exception {
        dbOps.processMessage(cancelAdmitPatient);

        assertEquals(0, plannedMovementRepository.count());
    }

    /**
     * A pending transfer creates a TRANSFER row, then an admission creates its own ADMISSION row.
     * Cancelling that admission should cancel only the ADMISSION row.
     */
    @Test
    void testCancelsOnlyTheAdmissionRow() throws Exception {
        dbOps.processMessage(pendingTransfer);
        dbOps.processMessage(admitPatient);
        dbOps.processMessage(cancelAdmitPatient);

        List<PlannedMovement> movements = plannedMovementRepository.findAllByHospitalVisitIdEncounter(VISIT_NUMBER);
        assertEquals(2, movements.size());

        PlannedMovement transferRow = movements.get(0);
        assertEquals("TRANSFER", transferRow.getEventType());
        assertFalse(transferRow.getCancelled());
        assertNull(transferRow.getCancelledDatetime());

        PlannedMovement admissionRow = movements.get(1);
        assertEquals("ADMISSION", admissionRow.getEventType());
        assertTrue(admissionRow.getCancelled());
        assertEquals(ADMISSION_EVENT_TIME, admissionRow.getCancelledDatetime());
    }

    /**
     * If no admission was ever processed, cancelling the admission must not touch the pre-existing
     * TRANSFER row, even though it's the most recent match at that location.
     */
    @Test
    void testDoesNotCancelTransferRow() throws Exception {
        dbOps.processMessage(pendingTransfer);
        dbOps.processMessage(cancelAdmitPatient);

        List<PlannedMovement> movements = plannedMovementRepository.findAllByHospitalVisitIdEncounter(VISIT_NUMBER);
        assertEquals(1, movements.size());
        assertEquals("TRANSFER", movements.get(0).getEventType());
        assertFalse(movements.get(0).getCancelled());
    }

    /**
     * Cancelling the same admission twice should be idempotent: no error, and the cancelledDatetime
     * set by the first call is not overwritten by the second.
     */
    @Test
    void testCancellingTwiceIsIdempotent() throws Exception {
        dbOps.processMessage(pendingTransfer);
        dbOps.processMessage(admitPatient);
        dbOps.processMessage(cancelAdmitPatient);
        dbOps.processMessage(cancelAdmitPatient);

        List<PlannedMovement> movements = plannedMovementRepository.findAllByHospitalVisitIdEncounter(VISIT_NUMBER);
        PlannedMovement admissionRow = movements.get(1);
        assertTrue(admissionRow.getCancelled());
        assertEquals(ADMISSION_EVENT_TIME, admissionRow.getCancelledDatetime());
    }
}
