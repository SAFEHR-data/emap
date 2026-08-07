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
import uk.ac.ucl.rits.inform.interchange.adt.AdmitPatient;
import uk.ac.ucl.rits.inform.interchange.adt.PendingTransfer;

import java.io.IOException;
import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * A real admission (ADT^A01) always records its own ADMISSION row in planned_movement,
 * per <a href="https://github.com/SAFEHR-data/emap/issues/166">#166</a>. Unlike the Z99/A08
 * hospital-service fallback, this never edits a matched row in place - it only links to it
 * via matchedMovementId.
 */
class TestAdmitPatientHospitalServiceFallback extends MessageProcessingBase {
    @Autowired
    private MrnRepository mrnRepository;
    @Autowired
    private CoreDemographicRepository coreDemographicRepository;
    @Autowired
    private HospitalVisitRepository hospitalVisitRepository;
    @Autowired
    private PlannedMovementRepository plannedMovementRepository;

    private AdmitPatient admitPatient;
    private PendingTransfer pendingTransfer;

    private static final String VISIT_NUMBER = "123412341234";
    private static final String LOCATION_STRING = "1020100166^SDEC BY02^11 SDEC";
    private static final Instant ADMISSION_EVENT_TIME = Instant.parse("2022-04-22T00:00:00Z");

    @BeforeEach
    void setup() throws IOException {
        admitPatient = messageFactory.getAdtMessage("generic/A01.yaml");
        admitPatient.setFullLocationString(InterchangeValue.buildFromHl7(LOCATION_STRING));
        admitPatient.setEventOccurredDateTime(ADMISSION_EVENT_TIME);

        pendingTransfer = messageFactory.getAdtMessage("pending/A15.yaml");
    }

    /**
     * Given that no planned movement exists at all (e.g. a direct A&E admission with no
     * preceding pending transfer request), an admission still inserts its own ADMISSION row,
     * with no matchedMovementId since there was nothing to fulfil.
     */
    @Test
    void testAdmissionInsertsOwnRowWithNoPriorMovement() throws Exception {
        dbOps.processMessage(admitPatient);

        assertEquals(1, mrnRepository.count());
        assertEquals(1, coreDemographicRepository.count());
        assertEquals(1, hospitalVisitRepository.count());

        List<PlannedMovement> movements = plannedMovementRepository.findAllByHospitalVisitIdEncounter(VISIT_NUMBER);
        assertEquals(1, movements.size());
        assertEquals("ADMISSION", movements.get(0).getEventType());
        assertEquals(ADMISSION_EVENT_TIME, movements.get(0).getEventDatetime());
        assertNull(movements.get(0).getMatchedMovementId());
    }

    /**
     * A prior pending transfer creates a TRANSFER row. An admission with the SAME hospital
     * service still inserts its own ADMISSION row (not a no-op), linking back to the TRANSFER
     * row via matchedMovementId - the TRANSFER row itself is left untouched.
     */
    @Test
    void testAdmissionInsertsOwnRowWhenHospitalServicesAreTheSame() throws Exception {
        dbOps.processMessage(pendingTransfer);
        admitPatient.setHospitalService(pendingTransfer.getHospitalService());
        dbOps.processMessage(admitPatient);

        List<PlannedMovement> movements = plannedMovementRepository.findAllByHospitalVisitIdEncounter(VISIT_NUMBER);
        assertEquals(2, movements.size());

        PlannedMovement transferRow = movements.get(0);
        assertEquals("TRANSFER", transferRow.getEventType());

        PlannedMovement admissionRow = movements.get(1);
        assertEquals("ADMISSION", admissionRow.getEventType());
        assertEquals(transferRow.getPlannedMovementId(), admissionRow.getMatchedMovementId());
        assertEquals(admitPatient.getHospitalService().get(), admissionRow.getHospitalService());
    }

    /**
     * Same as above, but with a differing hospital service - still just one new ADMISSION row,
     * not a separate EDIT/HOSPITAL_SERVICE_CHANGE row. The TRANSFER row is still untouched.
     */
    @Test
    void testAdmissionInsertsOwnRowWhenHospitalServicesDiffer() throws Exception {
        dbOps.processMessage(pendingTransfer);
        dbOps.processMessage(admitPatient);

        List<PlannedMovement> movements = plannedMovementRepository.findAllByHospitalVisitIdEncounter(VISIT_NUMBER);
        assertEquals(2, movements.size());

        PlannedMovement transferRow = movements.get(0);
        assertEquals("TRANSFER", transferRow.getEventType());

        PlannedMovement admissionRow = movements.get(1);
        assertEquals("ADMISSION", admissionRow.getEventType());
        assertEquals(transferRow.getPlannedMovementId(), admissionRow.getMatchedMovementId());
        assertEquals(admitPatient.getHospitalService().get(), admissionRow.getHospitalService());
    }

    /**
     * Reprocessing the identical admission message must not duplicate the ADMISSION row.
     */
    @Test
    void testReprocessingSameAdmissionIsIdempotent() throws Exception {
        dbOps.processMessage(admitPatient);
        dbOps.processMessage(admitPatient);

        List<PlannedMovement> movements = plannedMovementRepository.findAllByHospitalVisitIdEncounter(VISIT_NUMBER);
        assertEquals(1, movements.size());
        assertEquals("ADMISSION", movements.get(0).getEventType());
    }
}
