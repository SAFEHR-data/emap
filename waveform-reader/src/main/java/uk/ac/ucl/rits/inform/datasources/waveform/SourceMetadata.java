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
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * The source data (from HL7 messages) is not fully self-describing.
 * We need external metadata to tell us certain things about each data stream.
 */
@Component
public class SourceMetadata {
    private final Logger logger = LoggerFactory.getLogger(Hl7ParseAndSend.class);
    private static final Resource CSV_RESOURCE = new ClassPathResource("source-metadata/Device_Values_formatted.csv");
    private Map<String, SourceMetadataItem> metadataByStreamId = new HashMap<>();

    SourceMetadata() throws IOException {
        logger.info("Loading metadata from {}", CSV_RESOURCE);
        CsvMapper mapper = new CsvMapper();
        mapper.enable(CsvParser.Feature.WRAP_AS_ARRAY)
                .enable(CsvParser.Feature.TRIM_SPACES);
        CsvSchema schema = CsvSchema.emptySchema().withUseHeader(true);
        InputStreamReader inputStreamReader = new InputStreamReader(CSV_RESOURCE.getInputStream());
        MappingIterator<Map<String, String>> mappingIterator =
                mapper.readerFor(Map.class).with(schema).readValues(inputStreamReader);
        while (mappingIterator.hasNext()) {
            Map<String, String> row = mappingIterator.next();
            String key = row.get("value_hl7_id");
            Integer samplingRate;
            try {
                samplingRate = Integer.parseInt(row.get("frequency"));
            } catch (NumberFormatException e) {
                samplingRate = null;
            }
            String unit = row.get("value_unit_name");
            String description = row.get("value_name");
            SourceMetadataItem metadataItem = new SourceMetadataItem(key, description, unit, samplingRate);
            logger.debug("Metadata item: {}", metadataItem);
            if (!metadataItem.isUsable()) {
                logger.warn("Metadata item cannot be used for mapping: {}", metadataItem);
            }
            metadataByStreamId.put(key, metadataItem);
        }
        mappingIterator.close();
        logger.info("Loaded {} metadata items from {}", metadataByStreamId.size(), CSV_RESOURCE);
    }


    /**
     * Get metadata for the stream ID, if we know it (hence Optional).
     * @param streamId stream unique ID
     * @return metadata record wrapped in Optional
     */
    public Optional<SourceMetadataItem> getStreamMetadata(String streamId) {
        SourceMetadataItem metadata = metadataByStreamId.get(streamId);
        if (metadata == null) {
            return Optional.empty();
        }
        return Optional.of(new SourceMetadataItem(streamId, metadata.mappedStreamDescription(),
                metadata.unit(), metadata.samplingRate()));
    }
}

/**
 * Describes a source stream.
 * @param sourceStreamId the source stream unique Id, Eg. "52912"
 * @param mappedStreamDescription Description of the stream eg. "Airway Volume Waveform"
 * @param unit The unit relating to the value, eg. "mL"
 * @param samplingRate number of samples per second, eg. 50
 */
record SourceMetadataItem(String sourceStreamId, String mappedStreamDescription, String unit, Integer samplingRate) {
    // allow unusable to exist for the sake of better logging messages
    public boolean isUsable() {
        // We need to know the sampling rate so we can check the data is free of gaps, for one thing
        return samplingRate != null && unit != null;
    }
}
