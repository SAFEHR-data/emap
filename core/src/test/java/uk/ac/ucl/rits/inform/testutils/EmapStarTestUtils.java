package uk.ac.ucl.rits.inform.testutils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.stereotype.Component;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.transaction.annotation.Transactional;

import uk.ac.ucl.rits.inform.datasinks.emapstar.repos.EncounterRepository;
import uk.ac.ucl.rits.inform.informdb.OldAttributeKeyMap;
import uk.ac.ucl.rits.inform.informdb.Encounter;
import uk.ac.ucl.rits.inform.informdb.PatientFact;
import uk.ac.ucl.rits.inform.informdb.PatientProperty;

/**
 * Utility methods for making assertions about the contents of Emap-Star.
 * These are used by the (Interchange -> Emap-Star) test cases in
 * uk.ac.ucl.rits.inform.datasinks.emapstar, and by the end to end (HL7 ->->
 * Emap-Star) tests in uk.ac.ucl.rits.inform.tests.
 *
 * @author Jeremy Stein
 */
@Component
@ActiveProfiles("test")
@ComponentScan(basePackages = { "uk.ac.ucl.rits.inform.informdb" })
public class EmapStarTestUtils {

    @Autowired
    protected EncounterRepository encounterRepo;

    public EmapStarTestUtils() {
    }

    /**
     * Check that the encounter got loaded and has the right number of
     * bed visits, and that one of those visits matches the given expected values.
     *
     * @param expectedEncounter the encounter ID to look for
     * @param expectedTotalVisits How many bed/location visits in the encounter in total
     * @param expectedLocation where the patient is expected to be for one of their visits
     * @param expectedDischargeTime for this same visit, the expected discharged time, or null if it's expected to be still open
     * @return the bedVisit found
     */
    @Transactional
    public PatientFact _testVisitExistsWithLocation(String expectedEncounter, int expectedTotalVisits, String expectedLocation, Instant expectedDischargeTime) {
        List<PatientFact> validBedVisits = getLocationVisitsForEncounter(expectedEncounter, expectedTotalVisits);
        List<PatientFact> validBedVisitsAtLocation =
                validBedVisits.stream().filter(f -> f.getPropertyByAttribute(OldAttributeKeyMap.LOCATION).get(0).getValueAsString().equals(expectedLocation)).collect(Collectors.toList());
        assertEquals(expectedTotalVisits, validBedVisits.size());
        if (expectedTotalVisits == 0) {
            return null;
        }
        PatientFact bedVisit = validBedVisitsAtLocation.get(0);
        List<PatientProperty> location = bedVisit.getPropertyByAttribute(OldAttributeKeyMap.LOCATION, p -> p.isValid());
        assertEquals(1, location.size(), "There should be exactly one location property for an inpatient bed visit");
        PatientProperty loca = location.get(0);
        assertTrue(loca.isValid());
        assertEquals(expectedLocation, loca.getValueAsString(), "Bedded location not correct");

        List<PatientProperty> dischargeTimes = bedVisit.getPropertyByAttribute(OldAttributeKeyMap.DISCHARGE_TIME, p -> p.isValid());
        if (expectedDischargeTime == null) {
            assertEquals(0, dischargeTimes.size(), "There is an unexpected discharge");
        } else {
            PatientProperty disch = dischargeTimes.get(0);
            assertEquals(expectedDischargeTime, disch.getValueAsDatetime(), "Discharge time does not match");

        }
        return bedVisit;
    }

    /**
     * @param expectedEncounter the encounter, which must exist
     * @param expectedTotalVisits how many visits you expect to exist
     * @return all valid location visit facts (not hospital visits), sorted by arrival time
     */
    @Transactional
    public List<PatientFact> getLocationVisitsForEncounter(String expectedEncounter, int expectedTotalVisits) {
        Encounter enc = encounterRepo.findEncounterByEncounter(expectedEncounter);
        assertNotNull(enc, "encounter did not exist");
        Map<OldAttributeKeyMap, List<PatientFact>> factsAsMap = enc.getFactsGroupByType();
        assertTrue(!factsAsMap.isEmpty(), "Encounter has no patient facts");
        List<PatientFact> validBedVisits = factsAsMap.getOrDefault(OldAttributeKeyMap.BED_VISIT, new ArrayList<>()).stream()
                .filter(PatientFact::isValid).collect(Collectors.toList());
        assertEquals(expectedTotalVisits, validBedVisits.size());
        // sort by arrival time
        validBedVisits.sort(Comparator.comparing(
                vis -> vis.getPropertyByAttribute(OldAttributeKeyMap.ARRIVAL_TIME).get(0).getValueAsDatetime(),
                Comparator.nullsFirst(Instant::compareTo)));
        return validBedVisits;
    }

    /**
     * Test a collection of properties of the same type where we expect to see one
     * current one and one invalidated one. The invalidated one takes two rows to
     * represent so there should be three in all.
     *
     * @param allProperties            all the rows of this property type,
     *                                 regardless of storedness or validity
     * @param expectedOldValue         the actual value of the old property
     * @param expectedCurrentValue     the actual current value
     * @param expectedOldValidFrom     when did the old value become true
     * @param expectedOldValidUntil    when did the old value stop being true
     * @param expectedCurrentValidFrom when did the new value start being true
     */
    public <ValueType> void _testPropertyValuesOverTime(List<PatientProperty> allProperties, ValueType expectedOldValue,
            ValueType expectedCurrentValue, Instant expectedOldValidFrom, Instant expectedOldValidUntil,
            Instant expectedCurrentValidFrom) {
        assertEquals(3, allProperties.size());
        Map<Pair<Boolean, Boolean>, List<PatientProperty>> allPropertiesByValidityAndStoredness = allProperties.stream().collect(Collectors
                .groupingBy(dt -> new ImmutablePair<>(dt.getStoredUntil() == null, dt.getValidUntil() == null)));

        List<PatientProperty> allCurrent = allPropertiesByValidityAndStoredness.get(new ImmutablePair<>(true, true));
        assertEquals(1, allCurrent.size());
        assertEquals(expectedCurrentValue, allCurrent.get(0).getValue(expectedCurrentValue.getClass()));
        assertEquals(expectedCurrentValidFrom, allCurrent.get(0).getValidFrom());

        List<PatientProperty> allDeletedValid = allPropertiesByValidityAndStoredness.get(new ImmutablePair<>(false, true));
        assertEquals(1, allDeletedValid.size());
        assertEquals(expectedOldValue, allDeletedValid.get(0).getValue(expectedOldValue.getClass()));

        List<PatientProperty> allStoredInvalid = allPropertiesByValidityAndStoredness.get(new ImmutablePair<>(true, false));
        assertEquals(1, allStoredInvalid.size());
        assertEquals(expectedOldValue, allStoredInvalid.get(0).getValue(expectedOldValue.getClass()));
        assertEquals(expectedOldValidUntil, allStoredInvalid.get(0).getValidUntil());

        // Can't tell what the stored from/until timestamps should be (they will be created
        // at the time the test was run), but we can at least test that they have the right
        // relationships to each other.

        // These should abut exactly because the invalidation was one operation
        assertEquals(allDeletedValid.get(0).getStoredUntil(), allStoredInvalid.get(0).getStoredFrom());

        // Some events (eg. Patient class change) can occur in a single message, so the changeover
        // is instantaneous. Others (Cancel transfer + new correct transfer) happen in separate messages
        // so the deletion and recreation will have a slight gap between them. Therefore allow equal to or less than.
        assertFalse(allDeletedValid.get(0).getStoredUntil().isAfter(allCurrent.get(0).getStoredFrom()));
        assertFalse(allDeletedValid.get(0).getStoredFrom().isAfter(allDeletedValid.get(0).getStoredUntil()));

        assertEquals(expectedOldValidFrom, allStoredInvalid.get(0).getValidFrom());
    }

    /**
     * Generate a string showing the hierarchical fact/property structure of an
     * encounter, only down to the level required for microbiology sensitivities
     * (not truly recursive).
     *
     * @param encounterStr the encounter string to print for
     * @return the pretty string
     */
    public String prettyPrintEncounterFacts(String encounterStr) {
        StringBuilder sb = new StringBuilder();
        Encounter enc = encounterRepo.findEncounterByEncounter(encounterStr);
        sb.append(enc + "\n");
        List<PatientFact> facts = enc.getFacts();
        sb.append(String.format(" FACTS x %s:\n", facts.size()));
        for (PatientFact pf : facts) {
            sb.append(String.format("    FACT[TYPE=%s[%d]]:\n", pf.getFactType().getShortName(), pf.getFactType().getAttributeId()));
            List<PatientProperty> properties = pf.getProperties();
            if (properties != null) {
                sb.append(String.format("        PROPERTIES x %s:\n", properties.size()));
                for (PatientProperty pp : properties) {
                    sb.append(String.format("            %s\n", pp.toString()));
                }
            }
            List<PatientFact> childFacts = pf.getChildFacts();
            sb.append(String.format("        CHILDFACTS x %s:\n", childFacts.size()));
            for (PatientFact chFact : childFacts) {
                sb.append(String.format("            CFACT[TYPE=%s[%d]]:\n", chFact.getFactType().getShortName(), chFact.getFactType().getAttributeId()));
                List<PatientProperty> childProperties = chFact.getProperties();
                sb.append(String.format("                CF PROPERTIES x %s:\n", childProperties.size()));
                for (PatientProperty ppp : childProperties) {
                    sb.append(String.format("                   %s\n", ppp.toString()));
                }
            }
        }
        return sb.toString();
    }

}
