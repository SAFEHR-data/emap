package uk.ac.ucl.rits.inform.datasources.waveform.hl7parse;

import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Hl7Segment {
    private final Logger logger = LoggerFactory.getLogger(Hl7Segment.class);
    @Getter
    private final String segmentName;
    private final List<Hl7Segment> childSegments = new ArrayList<>();
    private final String[] fields;

    /**
     * Parse an HL7 segment to field level. Further parsing and all validation
     * is the responsibility of the calling code.
     * @param segment string containing the whole segment
     */
    public Hl7Segment(String segment) {
        String[] fields = segment.split("\\|");
        this.segmentName = fields[0];
        if (this.segmentName.equals("MSH")) {
            List<String> strings = new ArrayList<>(Arrays.asList(fields));
            // MSH-1 is the pipe character itself, which .split will miss
            strings.add(1, "|");
            fields = strings.toArray(new String[0]);
        }
        this.fields = fields;
        logger.trace("Segment: name = {}, fields = {}", this.segmentName, this.fields);
    }

    /**
     * Get unparsed contents of HL7 field for this segment.
     * @param field1Index The HL7 field index within the segment, starting at 1
     * @return unparsed string contents of the field
     * @throws Hl7ParseException if field does not exist
     */
    public String getField(int field1Index) throws Hl7ParseException {
        try {
            return fields[field1Index];
        } catch (ArrayIndexOutOfBoundsException e) {
            throw (Hl7ParseException) new Hl7ParseException("non existent field").initCause(e);
        }
    }

    public void addChildSegment(Hl7Segment seg) {
        childSegments.add(seg);
    }

    /**
     * @return all child segments
     */
    public List<Hl7Segment> getChildSegments() {
        return getChildSegments(null);

    }

    /**
     * Get all child segments with the given name.
     * @param segmentName name to match on, or null to return all
     * @return Matching child segments
     */
    public List<Hl7Segment> getChildSegments(String segmentName) {
        return childSegments.stream().filter(
                        cs -> segmentName == null || cs.getSegmentName().equals(segmentName))
                .toList();
    }

}
