package uk.ac.ucl.rits.inform.datasources.waveform;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Map Capsule location strings to the strings we get on the main HL7 ADT feed.
 */
@Component
public class LocationMapping {
    private final Logger logger = LoggerFactory.getLogger(LocationMapping.class);
    private final Map<Integer, List<Integer>> bayToBeds = Map.of(
            // derived from real data
            1, List.of(11, 12, 14, 15, 16),
            2, List.of(17, 18, 19, 20, 21),
            3, List.of(22, 23, 24, 25, 26),
            4, List.of(27, 28, 29, 30, 31),
            5, List.of(33, 34, 35, 36));
    private final Map<Integer, Integer> bayFromBed = new HashMap<>();

    LocationMapping() {
        for (var bayToBeds: bayToBeds.entrySet()) {
            Integer bay = bayToBeds.getKey();
            List<Integer> beds = bayToBeds.getValue();
            for (int bed: beds) {
                // mappings above must be unambiguous
                Integer existing = bayFromBed.put(bed, bay);
                if (existing != null) {
                    throw new RuntimeException(String.format(
                            "Ambiguity in bed->bay mapping, check your static data. %d maps to %d or %d",
                            bed, bay, existing));
                }
            }
        }
    }

    String hl7AdtLocationFromCapsuleLocation(String capsuleLocation) {
        final Pattern sideroomPattern = Pattern.compile("UCHT03ICURM(\\d+)");
        Matcher sideroomMatcher = sideroomPattern.matcher(capsuleLocation);
        if (sideroomMatcher.find()) {
            // side room schema
            int sideroomNumber = Integer.parseInt(sideroomMatcher.group(1));
            return String.format("T03^T03 SR%02d^SR%02d-%02d", sideroomNumber, sideroomNumber, sideroomNumber);
        } else {
            final Pattern bedPattern = Pattern.compile("UCHT03ICUBED(\\d+)");
            Matcher bedMatcher = bedPattern.matcher(capsuleLocation);
            if (bedMatcher.find()) {
                // bay+bed schema
                int bedNumber = Integer.parseInt(bedMatcher.group(1));
                Integer bayNumber = bayFromBed.get(bedNumber);
                if (bayNumber != null) {
                    return String.format("T03^T03 BY%02d^BY%02d-%02d", bayNumber, bayNumber, bedNumber);
                }
            }
        }
        logger.error("Could not map capsule location string {}", capsuleLocation);
        return null;
    }


}
