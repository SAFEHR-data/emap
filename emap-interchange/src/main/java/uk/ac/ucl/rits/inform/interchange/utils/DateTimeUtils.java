package uk.ac.ucl.rits.inform.interchange.utils;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

/**
 * Utility functions for manipulating timestamps etc.
 */
public final class DateTimeUtils {
    private DateTimeUtils() {}

    /**
     * Round Instant to the nearest time unit using normal convention of halfway rounds up.
     * @param instant Instant to round
     * @param roundToUnit what ChronoUnit to round to, or null to return the original timestamp
     * @return the rounded Instant
     */
    public static Instant roundInstantToNearest(Instant instant, ChronoUnit roundToUnit) {
        if (roundToUnit == null) {
            return instant;
        }
        // determine whether to round up or down
        Instant roundedDown = instant.truncatedTo(roundToUnit);
        Instant roundedUp = instant.plus(1, roundToUnit).truncatedTo(roundToUnit);
        if (instant.until(roundedUp, ChronoUnit.NANOS) > roundedDown.until(instant, ChronoUnit.NANOS)) {
            return roundedDown;
        } else {
            return roundedUp;
        }
    }

}
