package uk.ac.ucl.rits.inform.datasinks.emapstar.adt;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import uk.ac.ucl.rits.inform.datasinks.emapstar.MessageProcessingBase;
import uk.ac.ucl.rits.inform.datasinks.emapstar.repos.CoreDemographicRepository;
import uk.ac.ucl.rits.inform.datasinks.emapstar.repos.HospitalVisitRepository;
import uk.ac.ucl.rits.inform.datasinks.emapstar.repos.MrnRepository;
import uk.ac.ucl.rits.inform.datasinks.emapstar.repos.PlannedMovementRepository;
import uk.ac.ucl.rits.inform.informdb.movement.PlannedMovement;
import uk.ac.ucl.rits.inform.interchange.test.helpers.InterchangeMessageFactory;
import uk.ac.ucl.rits.inform.interchange.InterchangeValue;
import uk.ac.ucl.rits.inform.interchange.adt.AdtMessage;
import uk.ac.ucl.rits.inform.interchange.adt.CancelPendingDischarge;
import uk.ac.ucl.rits.inform.interchange.adt.CancelPendingTransfer;
import uk.ac.ucl.rits.inform.interchange.adt.PendingDischarge;
import uk.ac.ucl.rits.inform.interchange.adt.PendingTransfer;
import uk.ac.ucl.rits.inform.interchange.adt.PendingType;

import java.io.IOException;
import java.io.InvalidClassException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestPendingAdt extends MessageProcessingBase {
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
    private PendingTransfer pendingTransfer;
    private CancelPendingTransfer cancelPendingTransfer;
    private PendingDischarge pendingDischarge;
    private CancelPendingDischarge cancelPendingDischarge;

    private static final String VISIT_NUMBER = "123412341234";
    private static final String LOCATION_STRING = "1020100166^SDEC BY02^11 SDEC";
    private static final Instant EVENT_TIME = Instant.parse("2022-04-21T23:22:58Z");
    private static final Instant CANCEL_TIME = Instant.parse("2022-04-21T23:37:58Z");


    private PlannedMovement getPlannedMovementOrThrow(String visitNumber, String location) {
        return plannedMovementRepository
                .findByHospitalVisitIdEncounterAndLocationIdLocationString(visitNumber, location).orElseThrow();
    }

    @BeforeEach
    void setup() throws IOException {
        pendingTransfer = messageFactory.getAdtMessage("pending/A15.yaml");
        cancelPendingTransfer = messageFactory.getAdtMessage("pending/A26.yaml");
        pendingDischarge = messageFactory.getAdtMessage("pending/A16.yaml");
        cancelPendingDischarge = messageFactory.getAdtMessage("pending/A25.yaml");
    }

    /**
     * Given that no entities exist in the database
     * When a pending transfer message is processed
     * Mrn, core demographics and hospital visit entities should be created
     * @throws Exception shouldn't happen
     */
    @Test
    void testPendingCreatesOtherEntities() throws Exception {
        dbOps.processMessage(pendingTransfer);

        assertEquals(1, mrnRepository.count());
        assertEquals(1, coreDemographicRepository.count());
        assertEquals(1, hospitalVisitRepository.count());
    }

    /**
     * Given that no entities exist in the database
     * When a pending discharge message is processed
     * Mrn, core demographics and hospital visit entities should be created
     * @throws Exception shouldn't happen
     */
    @Test
    void testPendingDischargeCreatesOtherEntities() throws Exception {
        dbOps.processMessage(pendingDischarge);

        assertEquals(1, mrnRepository.count());
        assertEquals(1, coreDemographicRepository.count());
        assertEquals(1, hospitalVisitRepository.count());
    }

    /**
     * Given no planned movements in the database
     * When a pending transfer is processed
     * Then a new PlannedMovement should be created
     * @throws Exception shouldn't happen
     */
    @Test
    void testPendingCreatesPlannedMovement() throws Exception {
        dbOps.processMessage(pendingTransfer);

        PlannedMovement plannedMovement = getPlannedMovementOrThrow(VISIT_NUMBER, LOCATION_STRING);
        assertEquals(EVENT_TIME, plannedMovement.getEventDatetime());
        assertEquals(PendingType.TRANSFER.toString(), plannedMovement.getEventType());
        assertNull(plannedMovement.getCancelledDatetime());
        assertEquals("Head and Neck - OMFS", plannedMovement.getHospitalService());
    }

    /**
     * Given no planned movements in the database
     * When a pending transfer is processed
     * Then a new PlannedMovement should be created
     * @throws Exception shouldn't happen
     */
    @Test
    void testPendingDischargeCreatesPlannedMovement() throws Exception {
        dbOps.processMessage(pendingDischarge);

        PlannedMovement plannedMovement = getPlannedMovementOrThrow(VISIT_NUMBER, null);
        assertEquals(EVENT_TIME, plannedMovement.getEventDatetime());
        assertEquals(PendingType.DISCHARGE.toString(), plannedMovement.getEventType());
        assertNull(plannedMovement.getCancelledDatetime());
    }

    /**
     * Given no planned movements in the database
     * When a pending cancel is processed
     * Then a new PlannedMovement should be created with an unknown event datetime
     * @throws Exception shouldn't happen
     */
    @Test
    void testOnlyCancelPendingCreatesPlannedMovement() throws Exception {
        dbOps.processMessage(cancelPendingTransfer);

        PlannedMovement plannedMovement = getPlannedMovementOrThrow(VISIT_NUMBER, LOCATION_STRING);
        assertEquals(CANCEL_TIME, plannedMovement.getCancelledDatetime());
        assertEquals(PendingType.TRANSFER.toString(), plannedMovement.getEventType());
        assertNull(plannedMovement.getEventDatetime());
    }

    /**
     * Given no planned movements in the database
     * When a pending transfer then a pending cancel message is processed
     * Then the planned movement should have the correct event datetime and cancellation time
     * @throws Exception shouldn't happen
     */
    @Test
    void testRequestThenCancelInOrder() throws Exception {
        dbOps.processMessage(pendingTransfer);
        dbOps.processMessage(cancelPendingTransfer);

        PlannedMovement plannedMovement = getPlannedMovementOrThrow(VISIT_NUMBER, LOCATION_STRING);
        assertEquals(EVENT_TIME, plannedMovement.getEventDatetime());
        assertEquals(CANCEL_TIME, plannedMovement.getCancelledDatetime());
    }


    /**
     * Given that pairs of request and cancellation messages
     * When they are processed in different orders
     * Then only the right amount of requests should be created and they all should be cancelled
     * @param pendingMessages pending messages
     * @param expectedCount   expected number of requests after processing
     * @throws Exception shouldn't happen
     */
    @ParameterizedTest
    @ArgumentsSource(TestMessageStreamProvider.class)
    void testDuplicateRequestInOrder(Iterable<? extends AdtMessage> pendingMessages, Integer expectedCount) throws Exception {
        for (var msg : pendingMessages) {
            if (msg instanceof PendingTransfer) {
                dbOps.processMessage((PendingTransfer) msg);
            } else if (msg instanceof CancelPendingTransfer) {
                dbOps.processMessage((CancelPendingTransfer) msg);
            } else {
                throw new InvalidClassException("Unrecognised class of message");
            }
        }
        List<PlannedMovement> plannedMovements = plannedMovementRepository.findAllByHospitalVisitIdEncounter(VISIT_NUMBER);
        assertEquals(expectedCount, plannedMovements.size());
        for (PlannedMovement pm : plannedMovements) {
            logger.trace("Testing planned movement has values set correctly");
            assertNotNull(pm.getCancelledDatetime(), "Cancelled datetime shouldn't be null");
            assertNotNull(pm.getEventDatetime(), "Event datetime shouldn't be null");
            assertTrue(pm.getCancelled());
        }
    }

}


/**
 * Create a stream of messages (matched pairs of requests and cancelled in plausible orders) with expected number of final planned movements.
 */
class TestMessageStreamProvider implements ArgumentsProvider {
    private final InterchangeMessageFactory messageFactory = new InterchangeMessageFactory();

    private void addAnHour(PendingTransfer pendingTransfer) {
        Instant laterTime = pendingTransfer.getEventOccurredDateTime().plus(1, ChronoUnit.HOURS);
        pendingTransfer.setEventOccurredDateTime(laterTime);
    }

    private void addAnHour(CancelPendingTransfer cancelPending) {
        Instant laterTime = cancelPending.getCancelledDateTime().plus(1, ChronoUnit.HOURS);
        cancelPending.setEventOccurredDateTime(laterTime);
        cancelPending.setCancelledDateTime(laterTime);
    }

    @Override
    public Stream<? extends Arguments> provideArguments(ExtensionContext context) throws IOException {
        PendingTransfer pendingTransfer = messageFactory.getAdtMessage("pending/A15.yaml");
        PendingTransfer pendingTransferLater = messageFactory.getAdtMessage("pending/A15.yaml");
        addAnHour(pendingTransferLater);
        PendingTransfer transferNoLocation = messageFactory.getAdtMessage("pending/A15.yaml");
        transferNoLocation.setPendingDestination(InterchangeValue.unknown());


        CancelPendingTransfer cancelPending = messageFactory.getAdtMessage("pending/A26.yaml");
        CancelPendingTransfer cancelPendingLater = messageFactory.getAdtMessage("pending/A26.yaml");
        addAnHour(cancelPendingLater);
        CancelPendingTransfer cancelNoLocation = messageFactory.getAdtMessage("pending/A26.yaml");
        cancelNoLocation.setPendingDestination(InterchangeValue.unknown());

        return Stream.of(
                // simple case of create and cancel, should have single entity
                Arguments.of(List.of(pendingTransfer, cancelPending), 1),
                // cancel received before pending
                Arguments.of(List.of(cancelPending, pendingTransfer), 1),
                // request created and cancelled twice, in order
                Arguments.of(List.of(pendingTransfer, cancelPending, pendingTransferLater, cancelPendingLater), 2),
                // request created and cancelled twice, last pending request out of order
                Arguments.of(List.of(pendingTransfer, cancelPending, cancelPendingLater, pendingTransferLater), 2),
                // request created and cancelled twice, cancels all received before the pending
                Arguments.of(List.of(cancelPending, cancelPendingLater, pendingTransfer, pendingTransferLater), 2),
                // request created twice, cancelled twice
                Arguments.of(List.of(pendingTransfer, pendingTransferLater, cancelPending, cancelPendingLater), 2),
                // null location added then cancelled
                Arguments.of(List.of(transferNoLocation, cancelNoLocation), 1),
                // mix of null location and cancellations
                Arguments.of(List.of(transferNoLocation, pendingTransfer, pendingTransferLater, cancelNoLocation, cancelPending, cancelPendingLater), 3)
        );
    }
}
