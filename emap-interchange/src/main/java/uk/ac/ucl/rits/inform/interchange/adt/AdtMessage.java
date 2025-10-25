package uk.ac.ucl.rits.inform.interchange.adt;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import uk.ac.ucl.rits.inform.interchange.EmapOperationMessage;
import uk.ac.ucl.rits.inform.interchange.InterchangeValue;

import java.time.Instant;
import java.time.LocalDate;

/**
 * An interchange message describing patient movements or info. Closely corresponds
 * to the HL7 ADT message type.
 * @author Jeremy Stein
 */
@Data
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, property = "@class")
public abstract class AdtMessage extends EmapOperationMessage {
    private Instant recordedDateTime;
    private String eventReasonCode;
    private Instant eventOccurredDateTime;
    private String mrn;
    private String nhsNumber;
    private String visitNumber;
    private InterchangeValue<String> modeOfArrival = InterchangeValue.unknown();
    private InterchangeValue<String> ethnicGroup = InterchangeValue.unknown();
    private InterchangeValue<String> fullLocationString = InterchangeValue.unknown();
    private InterchangeValue<String> previousLocationString = InterchangeValue.unknown();
    private InterchangeValue<LocalDate> patientBirthDate = InterchangeValue.unknown();
    private InterchangeValue<Instant> patientBirthDateTime = InterchangeValue.unknown();
    private InterchangeValue<PatientClass> patientClass = InterchangeValue.unknown();
    private InterchangeValue<Instant> patientDeathDateTime = InterchangeValue.unknown();
    private InterchangeValue<Boolean> patientIsAlive = InterchangeValue.unknown();

    private InterchangeValue<String> patientFamilyName = InterchangeValue.unknown();
    private InterchangeValue<String> patientGivenName = InterchangeValue.unknown();
    private InterchangeValue<String> patientMiddleName = InterchangeValue.unknown();
    private InterchangeValue<String> patientReligion = InterchangeValue.unknown();
    private InterchangeValue<String> patientSex = InterchangeValue.unknown();
    private InterchangeValue<String> patientTitle = InterchangeValue.unknown();
    private InterchangeValue<String> patientZipOrPostalCode = InterchangeValue.unknown();


    /**
     * Ideally the time the event occurred, but uses the message date time as a backup.
     * If no event occurred, the message date time can be days delayed, but this is the best we can do with limited information.
     * @return valid from instant.
     */
    public Instant bestGuessAtValidFrom() {
        return (eventOccurredDateTime == null) ? recordedDateTime : eventOccurredDateTime;
    }
}
