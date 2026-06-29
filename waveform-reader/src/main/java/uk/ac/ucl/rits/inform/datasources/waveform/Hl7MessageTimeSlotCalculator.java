package uk.ac.ucl.rits.inform.datasources.waveform;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;

@Component
class Hl7MessageTimeSlotCalculator {
    private int timeSlotMinutes;

    /**
     * @param timeSlotMinutes Size of a slot in which to put all data in the same file.
     *                         Must be a factor of 60 (including 1 and 60) so that
     *                         we know the pattern repeats every hour. Ie. the 00 minute
     *                         slot will always exist.
     * @throws IllegalArgumentException if slot width is not a factor of 60
     */
    Hl7MessageTimeSlotCalculator(
            @Value("${waveform.hl7.save.archive_time_slot_minutes:1}") int timeSlotMinutes
    ) {
        if (timeSlotMinutes < 1 || timeSlotMinutes > 60 || (60 % timeSlotMinutes != 0)) {
            throw new IllegalArgumentException("slotWidth must be a factor of 60");
        }
        this.timeSlotMinutes = timeSlotMinutes;
    }

    /**
     * Truncate the given time down to the beginning of the time slot in which it should be archived.
     *
     * @param fullPrecisionTime the timestamp to truncate
     * @return an Instant with a minute value that is a multiple of slotWidthMinutes, and seconds and fractions of second at zero
     */
    public Instant truncateTime(Instant fullPrecisionTime) {
        // Instants don't support a lot of concepts, including minutes of the hour
        ZonedDateTime zonedDateTime = ZonedDateTime.ofInstant(fullPrecisionTime, ZoneId.of("UTC"));
        long minuteOfHour = zonedDateTime.getMinute();
        long newMinuteOfHour = minuteOfHour - minuteOfHour % this.timeSlotMinutes;
        // rounding will never change the hour due to the factor of 60 restriction
        ZonedDateTime rounded = zonedDateTime.truncatedTo(ChronoUnit.HOURS).with(ChronoField.MINUTE_OF_HOUR, newMinuteOfHour);
        return rounded.toInstant();
    }
}
