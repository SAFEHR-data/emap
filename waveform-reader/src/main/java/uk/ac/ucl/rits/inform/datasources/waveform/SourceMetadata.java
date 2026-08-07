package uk.ac.ucl.rits.inform.datasources.waveform;

import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvParser;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * The source data (from HL7 messages) is not fully self-describing.
 * We need external metadata to tell us certain things about each data variable.
 */
@Component
public class SourceMetadata {
    private final Logger logger = LoggerFactory.getLogger(Hl7ParseAndQueue.class);
    private static final Resource VARIABLE_CSV = new ClassPathResource("source-metadata/Device_Values_formatted.csv");
    private static final Resource CHANNELS_CSV = new ClassPathResource("source-metadata/Carescape parameters.csv");
    private static final Resource UNITS_CSV = new ClassPathResource("source-metadata/unit_codes.csv");
    private static final Resource VENTILATOR_MODE_CSV = new ClassPathResource("source-metadata/ventilator_mode_mapping.csv");
    private static final Resource INSPIRATORY_TRIGGER_TYPE_CSV = new ClassPathResource("source-metadata/inspiratory_trigger_type_value_mapping.csv");
    private Map<String, SourceMetadataItem> metadataByVariableId = new HashMap<>();
    private Map<String, String> unitsByCode = new HashMap<>();
    private Map<String, String> ventModes = new HashMap<>();
    private Map<String, String> inspTriggerTypes = new HashMap<>();

    SourceMetadata() throws IOException {
        logger.info("Loading metadata from {}", CHANNELS_CSV);
        MappingIterator<Map<String, String>> channelsMappingIterator = readCsv(CHANNELS_CSV);
        // Some variables use multiple channels (eg. ECG), described in a CSV file.
        // This is important for the correct interpretation of the OBR-13 field.
        Map<String, List<String>> variablesToChannels = new HashMap<>();
        while (channelsMappingIterator.hasNext()) {
            Map<String, String> row = channelsMappingIterator.next();
            String variable = row.get("Variable");
            String channels = row.get("Channel");
            List<String> channelsList = Arrays.stream(channels.strip().split("\\s*,\\s*")).toList();
            variablesToChannels.put(variable, channelsList);
        }
        channelsMappingIterator.close();
        logger.info("Loading metadata from {}", VARIABLE_CSV);
        MappingIterator<Map<String, String>> variablesMappingIterator = readCsv(VARIABLE_CSV);
        while (variablesMappingIterator.hasNext()) {
            Map<String, String> row = variablesMappingIterator.next();
            String key = row.get("value_hl7_id");
            Integer samplingRate;
            try {
                samplingRate = Integer.parseInt(row.get("frequency"));
            } catch (NumberFormatException e) {
                // not everything is a waveform!
                samplingRate = null;
            }
            String unit = row.get("value_unit_name");
            String description = row.get("value_name");
            String valueType = row.get("value_type");
            // Look up channels, if any.
            // If channels column is empty this means there are no channels used,
            // ie. treated same as if the row didn't exist in CHANNELS_CSV
            List<String> channels = variablesToChannels.getOrDefault(key, Collections.emptyList());
            SourceMetadataItem metadataItem = new SourceMetadataItem(key, description, unit, valueType, samplingRate, channels);
            logger.debug("Metadata item: {}", metadataItem);
            if (!metadataItem.isUsable()) {
                logger.warn("Metadata item cannot be used for mapping: {}", VARIABLE_CSV);
            }
            metadataByVariableId.put(key, metadataItem);
        }
        variablesMappingIterator.close();
        logger.info("Loaded {} metadata items from {}", metadataByVariableId.size(), VARIABLE_CSV);

        loadUnitCodes();
        loadVentModes();
        loadInspTriggerTypes();
    }

    private void loadUnitCodes() throws IOException {
        try (MappingIterator<Map<String, String>> unitCodesIterator = readCsv(UNITS_CSV)) {
            while (unitCodesIterator.hasNext()) {
                Map<String, String> row = unitCodesIterator.next();
                String code = row.get("UnitCode");
                String symbolicUnit = row.get("Short Description");
                if (code == null || symbolicUnit == null) {
                    throw new RuntimeException("Could not load units from CSV");
                }
                unitsByCode.put(code, symbolicUnit);
            }
        }
    }

    private void loadInspTriggerTypes() throws IOException {
        try (MappingIterator<Map<String, String>> inspTriggerTypesIterator = readCsv(INSPIRATORY_TRIGGER_TYPE_CSV)) {
            while (inspTriggerTypesIterator.hasNext()) {
                Map<String, String> row = inspTriggerTypesIterator.next();
                String variableValue = row.get("variable_value");
                String variableDescription = row.get("variable_description");
                if (variableValue == null || variableDescription == null) {
                    throw new RuntimeException("Could not load insp trigger types from CSV");
                }
                inspTriggerTypes.put(variableValue, variableDescription);
            }
        }
    }

    private void loadVentModes() throws IOException {
        try (MappingIterator<Map<String, String>> unitCodesIterator = readCsv(VENTILATOR_MODE_CSV)) {
            while (unitCodesIterator.hasNext()) {
                Map<String, String> row = unitCodesIterator.next();
                String ventModeValue = row.get("Value of Variable 584");
                String ventModeDesc = row.get("Variable Description");
                if (ventModeValue == null || ventModeDesc == null) {
                    throw new RuntimeException("Could not load VentMode from CSV");
                }
                ventModes.put(ventModeValue, ventModeDesc);
            }
        }
    }


    private static MappingIterator<Map<String, String>> readCsv(Resource csvToRead) throws IOException {
        CsvMapper mapper = new CsvMapper();
        mapper.enable(CsvParser.Feature.WRAP_AS_ARRAY)
                .enable(CsvParser.Feature.TRIM_SPACES);
        CsvSchema schema = CsvSchema.emptySchema().withUseHeader(true);
        InputStreamReader inputStreamReader = new InputStreamReader(csvToRead.getInputStream());
        MappingIterator<Map<String, String>> mappingIterator =
                mapper.readerFor(Map.class).with(schema).readValues(inputStreamReader);
        return mappingIterator;
    }


    /**
     * Get metadata for the variable ID, if we know it (hence Optional).
     * @param variableId variable unique ID
     * @return metadata record wrapped in Optional
     */
    public Optional<SourceMetadataItem> getVariableMetadata(String variableId) {
        return Optional.ofNullable(metadataByVariableId.get(variableId));
    }

    /**
     * @param code a string code from the HL7 message
     * @return a string representation of the unit if it exists
     */
    public Optional<String> getUnitFromCode(String code) {
        return Optional.ofNullable(unitsByCode.get(code));
    }

    /**
     * Try to map a categorical value from HL7.
     * @param variableId the variable ID
     * @param sourceValue the value of the variable that may be categorical
     * @return non-null Optional if successfully mapped, Optional.empty() if not a known categorical variable
     * @throws UnknownCategoricalValueException if is a known categorical variable, but the value is unknown for that variable
     */
    public Optional<String> tryMapCategorical(String variableId, String sourceValue) throws UnknownCategoricalValueException {
        if (variableId.equals("584")) {
            // Ventilation Mode
            String mappedValue = ventModes.get(sourceValue);
            if (mappedValue != null) {
                return Optional.of(mappedValue);
            }
        } else if (variableId.equals("2047")) {
            // 2047,Inspired Trigger Type,,Variable,Servo-U,TRUE,
            String mappedValue = inspTriggerTypes.get(sourceValue);
            if (mappedValue != null) {
                return Optional.of(mappedValue);
            }
        } else {
            // it's not a known categorical variable so you can assume it's a numerical
            return Optional.empty();
        }
        throw new UnknownCategoricalValueException(
                String.format("Categorical variable %s identified but value %s is invalid", variableId, sourceValue));
    }
}

class UnknownCategoricalValueException extends Exception {
    UnknownCategoricalValueException(String msg) {
        super(msg);
    }
}

/**
 * Describes a source variable.
 * @param sourceVariableId the source variable unique Id, Eg. "52912"
 * @param mappedVariableDescription Description of the variable eg. "Airway Volume Waveform"
 * @param unit The unit relating to the value, eg. "mL"
 * @param valueType possible values: Set Static Variable Waveform
 * @param samplingRate number of samples per second, eg. 50
 * @param channelIds the names of the expected channels for this variable. Can be null or empty...
 */
record SourceMetadataItem(
        String sourceVariableId,
        String mappedVariableDescription,
        String unit,
        String valueType,
        Integer samplingRate,
        List<String> channelIds) {
    // allow unusable to exist for the sake of better logging messages
    public boolean isUsable() {
        if (isWaveform()) {
            // We need to know the sampling rate so we can check the data is free of gaps, for one thing
            return samplingRate != null && unit != null;
        } else {
            return unit != null;
        }
    }

    public boolean isWaveform() {
        return valueType.equals("Waveform");
    }

    /**
     * Affects the interpretation of OBR-13.
     * @return whether the data is separated into channels.
     */
    public boolean hasChannels() {
        return !channelIds.isEmpty();
    }
}
