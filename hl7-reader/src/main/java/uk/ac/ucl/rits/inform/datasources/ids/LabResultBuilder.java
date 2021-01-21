package uk.ac.ucl.rits.inform.datasources.ids;

import ca.uhn.hl7v2.model.DataTypeException;
import ca.uhn.hl7v2.model.Type;
import ca.uhn.hl7v2.model.Varies;
import ca.uhn.hl7v2.model.v26.datatype.CE;
import ca.uhn.hl7v2.model.v26.datatype.CWE;
import ca.uhn.hl7v2.model.v26.datatype.FT;
import ca.uhn.hl7v2.model.v26.datatype.IS;
import ca.uhn.hl7v2.model.v26.datatype.NM;
import ca.uhn.hl7v2.model.v26.datatype.ST;
import ca.uhn.hl7v2.model.v26.datatype.TX;
import ca.uhn.hl7v2.model.v26.segment.NTE;
import ca.uhn.hl7v2.model.v26.segment.OBR;
import ca.uhn.hl7v2.model.v26.segment.OBX;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.ac.ucl.rits.inform.interchange.InterchangeValue;
import uk.ac.ucl.rits.inform.interchange.lab.LabResultMsg;
import uk.ac.ucl.rits.inform.interchange.lab.LabResultStatus;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Turn part of an HL7 lab result message into a (flatter) structure
 * more suited to our needs.
 * @author Jeremy Stein
 * @author Stef Piatek
 */
public class LabResultBuilder {
    private static final Logger logger = LoggerFactory.getLogger(LabResultBuilder.class);

    private LabResultMsg msg = new LabResultMsg();
    private final Pattern completePattern = Pattern.compile("COMPLETE: \\d{1,2}/\\d{1,2}/\\d{2,4}");

    /**
     * @return the underlying message we have now built
     */
    public LabResultMsg getMessage() {
        return msg;
    }

    /**
     * This class stores an individual result (i.e. OBX segment) and some patient information (OBR).
     * @param obx   the OBX segment for this result
     * @param obr   the OBR segment for this result (will be the same segment shared with other OBXs)
     * @param notes list of NTE segments for this result
     * @throws DataTypeException if required datetime fields cannot be parsed
     */
    public LabResultBuilder(OBX obx, OBR obr, List<NTE> notes) throws DataTypeException {
        // see HL7 Table 0125 for value types
        // In addition to NM (Numeric), we get (descending popularity):
        //     ED (Encapsulated Data), ST (String), FT (Formatted text - display),
        //     TX (Text data - display), DT (Date), CE (deprecated and replaced by CNE or CWE, coded entry with or without exceptions)
        msg.setValueType(obx.getObx2_ValueType().getValueOrEmpty());
        // OBR segments for sensitivities don't have an OBR-22 status change time
        // so use the time from the parent?
        msg.setResultTime(HL7Utils.interpretLocalTime(obr.getObr22_ResultsRptStatusChngDateTime()));

        // each result needs to know this so sensitivities can be correctly assigned
        msg.setEpicCareOrderNumber(obr.getObr2_PlacerOrderNumber().getEi1_EntityIdentifier().getValueOrEmpty());
        setTestIdentifiers(obx);
        populateResults(obx);
        populateComments(notes);
    }

    /**
     * Set test identifiers.
     * @param obx OBX segment
     */
    private void setTestIdentifiers(OBX obx) {
        CWE obx3 = obx.getObx3_ObservationIdentifier();
        msg.setTestItemLocalCode(obx3.getCwe1_Identifier().getValueOrEmpty());
        msg.setTestItemLocalDescription(obx3.getCwe2_Text().getValueOrEmpty());
        msg.setTestItemCodingSystem(obx3.getCwe3_NameOfCodingSystem().getValueOrEmpty());
    }

    /**
     * Populate OBX fields. Mainly tested where value type is NM - numeric.
     * @param obx the OBX segment
     */
    private void populateResults(OBX obx) {
        int repCount = obx.getObx5_ObservationValueReps();

        // The first rep is all that's needed for most data types
        Varies dataVaries = obx.getObx5_ObservationValue(0);
        Type data = dataVaries.getData();
        if (data instanceof ST
                || data instanceof FT
                || data instanceof TX
                || data instanceof NM) {
            setStringValue(obx);
            if (data instanceof NM) {
                if (repCount > 1) {
                    logger.warn(String.format("WARNING - is numerical (NM) result but repcount = %d", repCount));
                }
                try {
                    setNumericValueAndResultOperator(msg.getStringValue());
                } catch (NumberFormatException e) {
                    logger.debug(String.format("LabResult numeric result couldn't be parsed: %s", msg.getStringValue()));
                }
            }
        } else if (data instanceof CE) {
            if (repCount > 1) {
                logger.warn(String.format("WARNING - is coded (CE) result but repcount = %d", repCount));
            }
            setIsolateFields((CE) data);
        }
        // also need to handle case where (data instanceof ED)

        msg.setUnits(InterchangeValue.buildFromHl7(obx.getObx6_Units().getCwe1_Identifier().getValueOrEmpty()));
        setReferenceRange(obx);
        setAbnormalFlag(obx);
        setResultStatus(obx);
        msg.setObservationSubId(obx.getObx4_ObservationSubID().getValueOrEmpty());
    }

    private void setResultStatus(OBX obx) {
        try {
            msg.setResultStatus(LabResultStatus.findByHl7Code(obx.getObx11_ObservationResultStatus().getValueOrEmpty()));
        } catch (IllegalArgumentException e) {
            logger.warn("Could not parse the PatientClass", e);
        }
    }

    private void setAbnormalFlag(OBX obx) {
        StringBuilder abnormalFlags = new StringBuilder();
        // Can't find any example in database of multiple flags but keeping in iteration and warning in case this changes with new data types
        for (IS flag : obx.getObx8_AbnormalFlags()) {
            abnormalFlags.append(flag.getValueOrEmpty());
        }
        if (abnormalFlags.length() > 1) {
            logger.warn("LabResult had more than one abnormal flag: {}", abnormalFlags);
        }
        if (abnormalFlags.length() != 0) {
            msg.setAbnormalFlag(InterchangeValue.buildFromHl7(abnormalFlags.toString()));
        }
    }

    private void setIsolateFields(CE data) {
        // we are assuming that all coded data is an isolate, not a great assumption
        CE ceData = data;
        msg.setIsolateLocalCode(ceData.getCe1_Identifier().getValue());
        msg.setIsolateLocalDescription(ceData.getCe2_Text().getValue());
        // Isolate coding system should default to empty string
        String isolateCodingSystem = ceData.getCe3_NameOfCodingSystem().getValue();
        msg.setIsolateCodingSystem(isolateCodingSystem == null ? "" : isolateCodingSystem);
    }

    private void setStringValue(OBX obx) {
        // Store the string value for numeric types to allow for debugging in case new result operator needs to be added
        String stringValue = Arrays.stream(obx.getObx5_ObservationValue())
                .map(Varies::getData)
                .map(Type::toString)
                .filter(Objects::nonNull)
                .collect(Collectors.joining("\n"));

        msg.setStringValue(stringValue);
    }

    /**
     * Set numeric value and result operator (if required).
     * @param inputValue string value
     */
    private void setNumericValueAndResultOperator(String inputValue) {
        String value = inputValue;

        if (!value.isEmpty() && (value.charAt(0) == '>' || value.charAt(0) == '<')) {
            String resultOperator = value.substring(0, 1);
            msg.setResultOperator(resultOperator);
            value = value.substring(1);
        }
        Double numericValue = Double.parseDouble(value);

        msg.setNumericValue(InterchangeValue.buildFromHl7(numericValue));
    }

    /**
     * Set reference low and reference high fields from OBX7.
     * @param obx OBX segment
     */
    private void setReferenceRange(OBX obx) {
        String referenceRange = obx.getObx7_ReferencesRange().getValueOrEmpty();
        String[] rangeValues = referenceRange.split("-");
        if (rangeValues.length == 2) {
            Double lower = Double.parseDouble(rangeValues[0]);
            Double upper = Double.parseDouble(rangeValues[1]);
            msg.setReferenceLow(InterchangeValue.buildFromHl7(lower));
            msg.setReferenceHigh(InterchangeValue.buildFromHl7(upper));
        } else if (!referenceRange.isBlank()) {
            if (referenceRange.startsWith("<")) {
                Double upper = Double.parseDouble(referenceRange.replace("<", ""));
                msg.setReferenceHigh(InterchangeValue.buildFromHl7(upper));
                msg.setReferenceLow(InterchangeValue.delete());
            } else if (referenceRange.startsWith(">")) {
                Double lower = Double.parseDouble(referenceRange.replace(">", ""));
                msg.setReferenceHigh(InterchangeValue.delete());
                msg.setReferenceLow(InterchangeValue.buildFromHl7(lower));
            } else {
                logger.warn(String.format("LabResult reference range not recognised: %s", String.join("-", rangeValues)));
            }
        }
    }

    /**
     * Gather all the NTE segments that relate to this OBX and save as concatenated value.
     * Ignores NTE-1 for now.
     * @param notes all NTE segments for the observation
     */
    private void populateComments(List<NTE> notes) {
        Collection<String> allNotes = new ArrayList<>(notes.size());
        for (NTE nt : notes) {
            for (FT ft : nt.getNte3_Comment()) {
                allNotes.add(ft.getValueOrEmpty());
            }
        }
        msg.setNotes(InterchangeValue.buildFromHl7(String.join("\n", allNotes)));
    }

    /**
     * @return Does this observation contain only redundant information
     * that can be ignored? Eg. header and footer of a report intended
     * to be human-readable.
     */
    public boolean isIgnorable() {
        // this will need expanding as we discover new cases
        if ("URINE CULTURE REPORT".equals(msg.getStringValue()) || "FLUID CULTURE REPORT".equals(msg.getStringValue())) {
            return true;
        }

        return completePattern.matcher(msg.getStringValue()).matches();
    }

    /**
     * Merge another lab result into this one.
     * Eg. an adjacent OBX segment that is linked by a sub ID.
     * @param labResultMsg the other lab result to merge in
     */
    public void mergeResult(LabResultMsg labResultMsg) {
        // Will need to identify HOW to merge results.
        // Eg. identify that LabResultMsg contains an isolate,
        // so only copy the isolate fields from it.
        if (!labResultMsg.getIsolateLocalCode().isEmpty()) {
            msg.setIsolateLocalCode(labResultMsg.getIsolateLocalCode());
            msg.setIsolateLocalDescription(labResultMsg.getIsolateLocalDescription());
            msg.setIsolateCodingSystem(labResultMsg.getIsolateCodingSystem());
        }
    }
}
