package uk.ac.ucl.rits.inform.datasinks.emapstar.controllers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import uk.ac.ucl.rits.inform.datasinks.emapstar.exceptions.IncompatibleDatabaseStateException;
import uk.ac.ucl.rits.inform.datasinks.emapstar.repos.locations.BedFacilityRepository;
import uk.ac.ucl.rits.inform.datasinks.emapstar.repos.locations.BedRepository;
import uk.ac.ucl.rits.inform.datasinks.emapstar.repos.locations.BedStateRepository;
import uk.ac.ucl.rits.inform.datasinks.emapstar.repos.locations.DepartmentRepository;
import uk.ac.ucl.rits.inform.datasinks.emapstar.repos.locations.DepartmentStateRepository;
import uk.ac.ucl.rits.inform.datasinks.emapstar.repos.locations.LocationRepository;
import uk.ac.ucl.rits.inform.datasinks.emapstar.repos.locations.RoomRepository;
import uk.ac.ucl.rits.inform.datasinks.emapstar.repos.locations.RoomStateRepository;
import uk.ac.ucl.rits.inform.informdb.TemporalFrom;
import uk.ac.ucl.rits.inform.informdb.movement.Bed;
import uk.ac.ucl.rits.inform.informdb.movement.BedFacility;
import uk.ac.ucl.rits.inform.informdb.movement.BedState;
import uk.ac.ucl.rits.inform.informdb.movement.Department;
import uk.ac.ucl.rits.inform.informdb.movement.DepartmentState;
import uk.ac.ucl.rits.inform.informdb.movement.Location;
import uk.ac.ucl.rits.inform.informdb.movement.Room;
import uk.ac.ucl.rits.inform.informdb.movement.RoomState;
import uk.ac.ucl.rits.inform.interchange.location.BedMetadata;
import uk.ac.ucl.rits.inform.interchange.location.DepartmentMetadata;
import uk.ac.ucl.rits.inform.interchange.location.LocationMetadata;
import uk.ac.ucl.rits.inform.interchange.location.MinimalDepartment;
import uk.ac.ucl.rits.inform.interchange.location.RoomMetadata;

import javax.annotation.Resource;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;

/**
 * Adds or updates location metadata (department, room, bed pool, bed, and their states).
 * @author Stef Piatek
 */
@Component
public class LocationController {
    private static final Logger logger = LoggerFactory.getLogger(LocationController.class);

    private final LocationRepository locationRepo;
    @Resource
    private DepartmentController departmentController;
    @Resource
    private RoomController roomController;
    @Resource
    private BedController bedController;


    /**
     * Interaction with hospital locations.
     * @param locationRepo repository for Location
     */
    public LocationController(LocationRepository locationRepo) {
        this.locationRepo = locationRepo;
    }

    /**
     * Gets location entity by string if it exists, otherwise creates it.
     * @param locationString full location string.
     * @return Location entity
     */
    @Cacheable(value = "location", key = "{#locationString}")
    public Location getOrCreateLocation(String locationString) {
        logger.trace("** Querying for location {}", locationString);
        return locationRepo.findByLocationStringEquals(locationString)
                .orElseGet(() -> {
                    Location location = new Location(locationString);
                    return locationRepo.save(location);
                });
    }

    /**
     * Create department if it doesn't exist and update state.
     * <p>
     * Status is the only thing that can change for a department state and we're not expecting them to start with a valid from.
     * This means that the best we can do is order them in the order that we receive them and if the state has changed, make this the active state.
     * @param msg        message to be processed
     * @param storedFrom time that emap core started processing the message
     * @throws IncompatibleDatabaseStateException if previous state speciality is different from the database's previous speciality
     */
    @Transactional
    public void processMessage(DepartmentMetadata msg, Instant storedFrom) throws IncompatibleDatabaseStateException {
        Department dep = departmentController.getOrCreateFullDepartment(msg);

        createDepartmentOnlyLocationIfRequired(dep);
        departmentController.processDepartmentStates(msg, dep, storedFrom);
    }

    /**
     * Create location string with no room and bed if it doesn't already exist.
     * <p>
     * Useful to add because we derive all possible hl7 strings from EPIC,
     * but if rooms or beds exist for a department then it won't create a hl7 string for location for the
     * department alone.
     * @param department department entity
     */
    private void createDepartmentOnlyLocationIfRequired(Department department) {
        String departmentOnlyHl7 = String.format("%s^null^null", department.getHl7String());
        if (locationRepo.existsByLocationStringEquals(departmentOnlyHl7)) {
            return;
        }
        Location departmentLocation = getOrCreateLocation(departmentOnlyHl7);
        if (departmentLocation.getDepartmentId() == null) {
            departmentLocation.setDepartmentId(department);
            locationRepo.save(departmentLocation);
        }
    }


    /**
     * Process location metadata, saving and updating states for department, room and bed.
     * Updates location ForeignKeys if they don't already exist.
     * @param msg        message to process
     * @param storedFrom time that emap core started processing the message
     * @throws IncompatibleDatabaseStateException if static entities (department, room and bed) change from what the database knows about
     */
    @Transactional
    @CacheEvict(value = "location", key = "{#msg.hl7String}")
    public void processMessage(LocationMetadata msg, Instant storedFrom) throws IncompatibleDatabaseStateException {
        Location location = getOrCreateLocation(msg.getHl7String());
        Department department = departmentController.getOrCreateMinimalDepartment(msg);

        Room room = null;
        if (msg.getRoomMetadata() != null) {
            room = roomController.updateOrCreateRoomAndState(department, msg.getRoomMetadata(), storedFrom);
        }
        Bed bed = null;
        if (msg.getBedMetadata() != null) {
            bed = bedController.processBedStateAndFacility(room, msg.getBedMetadata(), storedFrom);
        }
        addLocationForeignKeys(location, department, room, bed);
    }


    /**
     * Add foreign keys to location if they are currently null and save.
     * @param location   location entity
     * @param department department entity
     * @param room       nullable room entity
     * @param bed        nullable bed entity
     * @throws IncompatibleDatabaseStateException if an existing foreign key has changed
     */
    private void addLocationForeignKeys(Location location, Department department, @Nullable Room room, @Nullable Bed bed)
            throws IncompatibleDatabaseStateException {
        boolean changed = false;
        if (location.getDepartmentId() != department) {
            if (location.getDepartmentId() != null) {
                throw new IncompatibleDatabaseStateException("A location string can't change it's Department");
            }
            changed = true;
            location.setDepartmentId(department);
        }

        if (room != null && room != location.getRoomId()) {
            if (location.getRoomId() != null) {
                throw new IncompatibleDatabaseStateException("A location string can't change it's Room");
            }
            changed = true;
            location.setRoomId(room);
        }
        if (bed != null && bed != location.getBedId()) {
            if (location.getBedId() != null) {
                throw new IncompatibleDatabaseStateException("A location string can't change it's Bed");
            }
            changed = true;
            location.setBedId(bed);
        }

        if (changed) {
            locationRepo.save(location);
        }
    }
}

/**
 * Adds or updates department information.
 * @author Stef Piatek
 */
@Component
class DepartmentController {
    private final DepartmentStateRepository departmentStateRepo;
    private final DepartmentRepository departmentRepo;
    private static final Logger logger = LoggerFactory.getLogger(DepartmentController.class);


    DepartmentController(DepartmentStateRepository departmentStateRepo, DepartmentRepository departmentRepo) {
        this.departmentStateRepo = departmentStateRepo;
        this.departmentRepo = departmentRepo;
    }

    /**
     * Get or create minimal department entity.
     * @param msg minimal department message
     * @return saved department entity
     */
    Department getOrCreateMinimalDepartment(MinimalDepartment msg) {
        return departmentRepo
                .findByInternalId(msg.getDepartmentId())
                .orElseGet(() -> departmentRepo.save(
                        new Department(msg.getDepartmentId())));
    }

    /**
     * Get or create department entity and update fields if they're different.
     * @param msg DepartmentMetadata
     * @return saved department entity
     * @throws IncompatibleDatabaseStateException if department name or Hl7 string changes
     */
    Department getOrCreateFullDepartment(DepartmentMetadata msg) throws IncompatibleDatabaseStateException {
        Department department = departmentRepo
                .findByInternalId(msg.getDepartmentId())
                .orElseGet(() -> departmentRepo.save(new Department(msg.getDepartmentId())));
        addMissingDataAndSave(msg, department);
        return department;
    }

    private void addMissingDataAndSave(DepartmentMetadata msg, Department department) throws IncompatibleDatabaseStateException {
        boolean updated = updateFieldIfNull(department.getHl7String(), msg.getDepartmentHl7(), department::setHl7String);
        updated = updateFieldIfNull(department.getName(), msg.getDepartmentName(), department::setName) || updated;
        if (updated) {
            departmentRepo.save(department);
        }
    }

    private static boolean updateFieldIfNull(String currentData, String newdata, Consumer<String> setter) throws IncompatibleDatabaseStateException {
        if (!Objects.equals(currentData, newdata)) {
            if (currentData != null) {
                String errorMessage = String.format("Unexpected department name change '%s' to '%s'", currentData, newdata);
                throw new IncompatibleDatabaseStateException(errorMessage);
            }
            setter.accept(newdata);
            return true;
        }
        return false;
    }


    /**
     * Create state from department and if it's different from an existing state, invalidate the previous state.
     * <p>
     * If a state exists that's valid after the current state, the created state will be invalidated.
     * @param msg        message to process
     * @param department parent department entity
     * @param storedFrom time that emap core started processing the message
     * @throws IncompatibleDatabaseStateException if previous state speciality is different from the database's previous speciality
     */
    void processDepartmentStates(DepartmentMetadata msg, Department department, Instant storedFrom) throws IncompatibleDatabaseStateException {
        Instant validFrom = msg.getSpecialityUpdate() == null ? msg.getDepartmentContactDate() : msg.getSpecialityUpdate();
        DepartmentState currentState = new DepartmentState(
            department, msg.getDepartmentRecordStatus().toString(),
            msg.getDepartmentSpeciality(), msg.getDepartmentType(),
            msg.getIsWardOrFlowArea(), msg.getIsCoreInpatientArea(),
            validFrom, storedFrom);

        if (departmentStateRepo.existsByDepartmentIdAndSpecialityAndValidFrom(department, msg.getDepartmentSpeciality(), validFrom)) {
            logger.debug("Department State already exists in the database, no need to process further");
            return;
        }

        Optional<DepartmentState> possiblePreviousState = departmentStateRepo
                .findFirstByDepartmentIdAndValidFromLessThanOrderByValidFromDesc(department, validFrom);
        Optional<DepartmentState> possibleNextState = departmentStateRepo
                .findFirstByDepartmentIdAndValidFromGreaterThanOrderByValidFrom(department, validFrom);

        // if a state already exists and is different from the current state then we should make a new valid state from the current message
        if (possiblePreviousState.isPresent()) {
            invalidatePreviousStateIfChanged(msg.getPreviousDepartmentSpeciality(), currentState, possiblePreviousState.get());
        } else if (msg.getPreviousDepartmentSpeciality() != null) {
            // if the previous department speciality is not in the database
            DepartmentState previousState = new DepartmentState(department,
                msg.getDepartmentRecordStatus().toString(),
                msg.getPreviousDepartmentSpeciality(), msg.getDepartmentType(),
                msg.getIsWardOrFlowArea(), msg.getIsCoreInpatientArea(),
                msg.getDepartmentContactDate(), storedFrom);
            previousState.setStoredUntil(currentState.getStoredFrom());
            previousState.setValidUntil(currentState.getValidFrom());
            departmentStateRepo.saveAll(List.of(previousState, currentState));
        } else {
            // previous state doesn't exist
            if (possibleNextState.isPresent() && !stateIsDifferent(currentState, possibleNextState.get())) {
                // next state is the same as the current, should just update the next state to be valid earlier, at the current state's valid from
                possibleNextState.get().setValidFrom(currentState.getValidFrom());
            } else {
                // no previous state, so just save the current state
                departmentStateRepo.save(currentState);
            }
        }

        // if there's a next state and its different, then we should invalidate the current state
        if (possibleNextState.isPresent() && stateIsDifferent(currentState, possibleNextState.get())) {
            currentState.setStoredUntil(storedFrom);
            currentState.setValidUntil(possibleNextState.get().getValidFrom());
            departmentStateRepo.save(currentState);
        }
    }


    private void invalidatePreviousStateIfChanged(String msgPreviousSpeciality, DepartmentState currentState, DepartmentState previousState)
            throws IncompatibleDatabaseStateException {
        // if not different, the current state doesn't get saved
        if (stateIsDifferent(currentState, previousState)) {
            if (msgPreviousSpeciality != null && !msgPreviousSpeciality.equals(previousState.getSpeciality())) {
                String errorMsg = String.format(
                        "Previous Department speciality is different from the database speciality. Database previous: %s, Message previous: %s",
                        previousState.getSpeciality(), msgPreviousSpeciality
                );
                logger.error(errorMsg);
                throw new IncompatibleDatabaseStateException(errorMsg);
            }
            // Add new department states to EMAP
            previousState.setStoredUntil(currentState.getStoredFrom());
            previousState.setValidUntil(currentState.getValidFrom());
            departmentStateRepo.saveAll(List.of(previousState, currentState));
        }
    }

    private boolean stateIsDifferent(DepartmentState currentState, DepartmentState previousState) {
        return !previousState.getStatus().equals(currentState.getStatus())
                || !Objects.equals(currentState.getSpeciality(), previousState.getSpeciality());
    }

}

/**
 * Adds or updates room information.
 * @author Stef Piatek
 */
@Component
class RoomController {
    private final RoomRepository roomRepo;
    private final RoomStateRepository roomStateRepo;

    RoomController(RoomRepository roomRepo, RoomStateRepository roomStateRepo) {
        this.roomRepo = roomRepo;
        this.roomStateRepo = roomStateRepo;
    }

    /**
     * Create Room if it doesn't exist and update state.
     * <p>
     * We should receive rooms in order of their valid from, so if a room doesn't exist (by CSN) then it should be created.
     * @param department department that the room belongs to
     * @param msg        message to be processed
     * @param storedFrom time that emap core started processing the message
     * @return room
     * @throws IncompatibleDatabaseStateException if room name changes
     */
    Room updateOrCreateRoomAndState(Department department, RoomMetadata msg, Instant storedFrom)
            throws IncompatibleDatabaseStateException {
        Room room = roomRepo
                .findByHl7StringAndDepartmentId(msg.getRoomHl7(), department)
                .orElseGet(() -> roomRepo.save(new Room(msg.getRoomHl7(), msg.getRoomName(), department)));

        if (notNullAndDifferent(room.getName(), msg.getRoomName())) {
            throw new IncompatibleDatabaseStateException("Room can't change it's name");
        }
        List<RoomState> states = roomStateRepo.findAllByRoomIdOrderByValidFromDesc(room);

        Optional<RoomState> existingState = states.stream()
                .filter(state -> state.getCsn().equals(msg.getRoomCsn()))
                .findFirst();

        if (existingState.isPresent()) {
            return room;
        }
        createCurrentStateAndInvalidatePrevious(msg, storedFrom, room, states);

        return room;
    }

    private boolean notNullAndDifferent(String msg, String dep) {
        return dep != null && !dep.equals(msg);
    }

    /**
     * Create new state from current message, invalidating the previous state and saving if required.
     * @param msg        message to process
     * @param storedFrom time that emap-core started processing the message
     * @param room       room entity
     * @param states     previous states sorted by descending valid from dates
     * @throws IncompatibleDatabaseStateException if a novel CSN is found with a contact date earlier than the latest state
     */
    private void createCurrentStateAndInvalidatePrevious(
            RoomMetadata msg, Instant storedFrom, Room room, Collection<RoomState> states) throws IncompatibleDatabaseStateException {
        RoomState currentState = new RoomState(
                room, msg.getRoomCsn(), msg.getRoomRecordState().toString(), msg.getIsRoomReady(), msg.getRoomContactDate(), storedFrom);


        // if the room doesn't have any existing states we don't need to invalidate any previous states
        if (states.isEmpty()) {
            roomStateRepo.save(currentState);
            return;
        }

        // assuming the current message is after the most recent state, we should invalidate it and save the new state
        RoomState previousState = states.stream().findFirst().orElseThrow();
        if (currentState.getValidFrom().isBefore(previousState.getValidFrom())) {
            throw new IncompatibleDatabaseStateException("New room state is valid before the most current room state");
        }

        previousState.setValidUntil(currentState.getValidFrom());
        previousState.setStoredUntil(currentState.getStoredFrom());

        roomStateRepo.saveAll(List.of(previousState, currentState));
    }

}


/**
 * Adds or updates bed information.
 * @author Stef Piatek
 */
@Component
class BedController {
    private final BedRepository bedRepo;
    private final BedStateRepository bedStateRepo;
    private final BedFacilityRepository bedFacilityRepo;


    /**
     * @param bedRepo         bed repository
     * @param bedStateRepo    bed state repository
     * @param bedFacilityRepo bed facility repository
     */
    BedController(BedRepository bedRepo, BedStateRepository bedStateRepo, BedFacilityRepository bedFacilityRepo) {
        this.bedRepo = bedRepo;
        this.bedStateRepo = bedStateRepo;
        this.bedFacilityRepo = bedFacilityRepo;
    }

    /**
     * Create Bed if it doesn't exist and update state and create facilities if required.
     * <p>
     * We should receive beds in order of their valid from, so if a bed doesn't exist (by CSN) then it should be created.
     * <p>
     * For pool beds, we create a single bed and in the state entity, increment the number of pool beds found at the contact time.
     * Because the CSN is only of the first encountered, an existing pool bed is found by those which have a pool bed count and the same
     * contact time. This means that if the locations are processed from the beginning of epic time again then the pool bed count will
     * be larger than the real value. This is fine because we shouldn't be removing the current progress from the locations hoover.
     * @param room       room entity that the bed is associated with
     * @param msg        message to be processed
     * @param storedFrom time that emap core started processing the message
     * @return bed
     * @throws IncompatibleDatabaseStateException if a new state is encountered which is has an earlier valid from than the most recent state
     */
    public Bed processBedStateAndFacility(Room room, BedMetadata msg, Instant storedFrom) throws IncompatibleDatabaseStateException {
        Bed bed = bedRepo
                .findByHl7StringAndRoomId(msg.getBedHl7(), room)
                .orElseGet(() -> bedRepo.save(new Bed(msg.getBedHl7(), room)));

        BedState bedState = processBedState(bed, msg, storedFrom);

        if (msg.getBedFacility() != null) {
            createBedFacilityIfNotExists(bedState, msg.getBedFacility());
        }

        return bed;
    }

    private BedState processBedState(Bed bed, BedMetadata msg, Instant storedFrom) throws IncompatibleDatabaseStateException {


        List<BedState> states = bedStateRepo.findAllByBedIdOrderByValidFromDesc(bed);

        // if we already know about the bed pool, increment it and don't do any further processing
        if (msg.getIsPoolBed() != null && msg.getIsPoolBed()) {
            Optional<BedState> existingPoolBed = findExistingPoolBedByValidFrom(msg.getBedContactDate(), states);
            if (existingPoolBed.isPresent()) {
                incrementPoolBedAndSave(existingPoolBed.get());
                return existingPoolBed.get();
            }
        }

        // if we already know about the bed CSN, don't do any further processing
        Optional<BedState> existingState = states.stream()
                .filter(state -> state.getCsn().equals(msg.getBedCsn()))
                .findFirst();
        if (existingState.isPresent()) {
            return existingState.get();
        }

        return createCurrentStateAndInvalidatePrevious(msg, bed, states, new TemporalFrom(msg.getBedContactDate(), storedFrom));
    }

    private Optional<BedState> findExistingPoolBedByValidFrom(Instant bedContactDate, Collection<BedState> states) {
        return states.stream()
                .filter(state -> state.getPoolBedCount() != null && state.getValidFrom().equals(bedContactDate))
                .findFirst();
    }

    private void incrementPoolBedAndSave(BedState existingPoolBed) {
        existingPoolBed.incrementPoolBedCount();
        bedStateRepo.save(existingPoolBed);
    }

    /**
     * Create new state from current message, invalidating the previous state and saving if required.
     * @param msg          message to process
     * @param bed          bed entity
     * @param states       previous states sorted by descending valid from dates
     * @param temporalFrom valid and stored from
     * @return Current bed state for message
     * @throws IncompatibleDatabaseStateException if a novel, non-pool CSN is found with a contact date earlier than the latest state
     */
    private BedState createCurrentStateAndInvalidatePrevious(
            BedMetadata msg, Bed bed, Collection<BedState> states, TemporalFrom temporalFrom) throws IncompatibleDatabaseStateException {
        BedState currentState = new BedState(
                bed, msg.getBedCsn(), msg.getBedIsInCensus(), msg.getIsBunkBed(),
                msg.getBedRecordState().toString(), msg.getIsPoolBed(), temporalFrom
        );

        if (msg.getIsPoolBed()) {
            incrementPoolBedAndSave(currentState);
        }

        // if the bed doesn't have any existing states we don't need to invalidate any previous states
        if (states.isEmpty()) {
            return bedStateRepo.save(currentState);
        }

        // assuming the current message is after the most recent state, we should invalidate it and save the new state
        BedState previousState = states.stream().findFirst().orElseThrow();
        if (currentState.getValidFrom().isBefore(previousState.getValidFrom())) {
            throw new IncompatibleDatabaseStateException("New bed state is valid before the most current bed state");
        }

        previousState.setValidUntil(temporalFrom.getValid());
        previousState.setStoredUntil(temporalFrom.getStored());

        bedStateRepo.saveAll(List.of(previousState, currentState));
        return currentState;
    }

    private void createBedFacilityIfNotExists(BedState bedState, String bedFacility) {
        bedFacilityRepo.findByBedStateIdAndType(bedState, bedFacility)
                .orElseGet(() -> bedFacilityRepo.save(new BedFacility(bedState, bedFacility)));
    }

}
