package uk.ac.ucl.rits.inform.datasources.waveform_generator.patient_model;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.ac.ucl.rits.inform.datasources.waveform.LocationMapping;
import uk.ac.ucl.rits.inform.interchange.adt.AdtMessage;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;

public class PatientLocationModel {
    private final Logger logger = LoggerFactory.getLogger(PatientLocationModel.class.getName());
    private final List<String> allPossibleLocations;
    private final Random random = new Random(234);
    private final LocationMapping locationMapping = new LocationMapping();

    // patients who we have transferred out but we might want to transfer back in again
    private final List<PatientDetails> patientsInOtherHospitalLocations = new ArrayList<>();

    // initialise all locations to have a patient
    private final Map<String, PatientDetails> locationToPatient = new HashMap<>();

    /**
     * Start a simulation of all patients.
     * @param allPossibleLocations all locations patients can inhabit (Capsule format)
     * @param nowTime Initial admit time for the first batch of patients.
     */
    public PatientLocationModel(final List<String> allPossibleLocations, Instant nowTime) {
        this.allPossibleLocations = new ArrayList<>(allPossibleLocations);
        allPossibleLocations.forEach(location -> locationToPatient.put(location, new PatientDetails(nowTime)));
    }

    /**
     * Randomly change stuff and return the changes such that the caller can generate the appropriate
     * ADT messages.
     * @param nowTime time at which ADT changes will be simulated to happen
     * @return description of changes made
     */
    public List<AdtMessage> makeModifications(Instant nowTime) {
        List<PatientDetails> admitList = new ArrayList<>();
        List<PatientDetails> dischargeList = new ArrayList<>();
        List<PatientDetails> transferList = new ArrayList<>();

        // probability of each patient being discharged/transferred at each time step
        final float probOfDischarge = 0.01F;
        final float probOfTransferOut = 0.01F;
        final float probOfTransferWithin = 0.01F;
        final float probOfTransferIn = 0.01F;

        // go through all locations and randomly move some patients out
        for (int i = 0; i < allPossibleLocations.size(); i++) {
            var thisLocation = allPossibleLocations.get(i);
            var thisPatient = locationToPatient.get(thisLocation);
            if (thisPatient == null) {
                // no patient to move
                continue;
            }
            if (random.nextFloat() < probOfDischarge) {
                // discharge, never to be seen again
                thisPatient.setLocation(thisLocation);
                thisPatient.setEventDatetime(nowTime);
                dischargeList.add(thisPatient);
                locationToPatient.put(thisLocation, null);
                logger.info("ADT: Discharge from location {} to external", thisLocation);
            } else if (random.nextFloat() < probOfTransferOut) {
                // discharge from ICU, with possibility of coming back
                thisPatient.setLocation("SomewhereNotIcu");
                thisPatient.setEventDatetime(nowTime);
                transferList.add(thisPatient);
                locationToPatient.put(thisLocation, null);
                logger.info("ADT: Discharge from location {} to other hospital location", thisLocation);
            }
        }
        // Now that it's slightly emptier, randomly move patients around/in
        // Move each patient to another ICU location with certain probability.
        for (int i = 0; i < allPossibleLocations.size(); i++) {
            var thisLocation = allPossibleLocations.get(i);
            var thisPatient = locationToPatient.get(thisLocation);
            if (thisPatient != null) {
                if (random.nextFloat() < probOfTransferWithin) {
                    Optional<String> randomEmptyLocation = findRandomEmptyLocation();
                    // if all ICU locations are full then just don't move them
                    if (randomEmptyLocation.isPresent()) {
                        String newLocation = randomEmptyLocation.get();
                        thisPatient.setEventDatetime(nowTime);
                        thisPatient.setLocation(newLocation);
                        transferList.add(thisPatient);
                        locationToPatient.put(newLocation, thisPatient);
                        locationToPatient.put(thisLocation, null);
                        logger.info("ADT: Move from location {} to location {}", thisLocation, newLocation);
                    }
                }
            }
        }

        // add patients
        for (int i = 0; i < allPossibleLocations.size(); i++) {
            var thisLocation = allPossibleLocations.get(i);
            var thisPatient = locationToPatient.get(thisLocation);
            if (thisPatient == null) {
                // maybe fill this slot with a new patient or one in another part of the hospital
                if (random.nextFloat() < probOfTransferIn) {
                    if (!patientsInOtherHospitalLocations.isEmpty()) {
                        PatientDetails incomingPatient = patientsInOtherHospitalLocations.remove(0);
                        // they are now back under our control so we know where they are
                        incomingPatient.clearLastEvent();
                        incomingPatient.setEventDatetime(nowTime);
                        incomingPatient.setLocation(thisLocation);
                        admitList.add(incomingPatient);
                        locationToPatient.put(thisLocation, incomingPatient);
                        logger.info("ADT: Move patient into location {} from other hospital location", thisLocation);
                    }
                } else if (random.nextFloat() < probOfTransferIn) {
                    PatientDetails newPatient = new PatientDetails(nowTime);
                    newPatient.setEventDatetime(nowTime);
                    newPatient.setLocation(thisLocation);
                    locationToPatient.put(thisLocation, newPatient);
                    admitList.add(newPatient);
                    logger.info("ADT: Move brand new patient into location {}", thisLocation);
                }
            }
        }
        List<AdtMessage> mods = new ArrayList<>();
        admitList.forEach(adm -> mods.add(adm.makeAdmitMessage()));
        transferList.forEach(tr -> mods.add(tr.makeTransferMessage()));
        dischargeList.forEach(disch -> mods.add(disch.makeDischargeMessage()));
        return mods;
    }

    private Optional<String> findRandomEmptyLocation() {
        var allEmpties = locationToPatient.entrySet().stream()
                .filter(entry -> entry.getValue() == null)
                .toList();
        if (allEmpties.isEmpty()) {
            return Optional.empty();
        }
        int randomIndex = random.nextInt(allEmpties.size());
        return Optional.of(allEmpties.get(randomIndex).getKey());
    }

    /**
     * Get patient details at the given location.
     * @param location in Capsule format
     * @return patient details
     */
    public PatientDetails getPatientForLocation(String location) {
        // Get current. Consider some sort of "get only if changed" option
        return locationToPatient.get(location);
    }
}
