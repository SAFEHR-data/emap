package uk.ac.ucl.rits.inform.datasources.waveform.hl7parse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * A very basic HL7 parser that currently is only tested with ORU^R01 messages.
 * I tried HAPI but found it to be much too slow for messages with a lot of data in;
 * it seems to copy data to structures that aren't what I want as final output (Varies[]),
 * whereas this parser doesn't attempt to process the contents of any fields, allowing
 * the calling code to do as it wishes.
 * It's about 100-1000x faster.
 */
public class Hl7Message {
    private final Logger logger = LoggerFactory.getLogger(Hl7Message.class);
    /**
     * All segments in the order they were encountered.
     */
    private final List<Hl7Segment> segments = new ArrayList<>();

    /**
     * All segments, nested according to certain rules (not all HL7 formats are implemented).
     * Insert order is preserved.
     */
    private final SortedMap<String, List<Hl7Segment>> segmentsBySegmentName = new TreeMap<>();

    /**
     * Define the parent segment for every segment that should be nested.
     * For every segment KEY encountered, it will be added as a child segment of the
     * most recently encountered segment VALUE.
     */
    private static final Map<String, String> CHILD_TO_PARENT = new HashMap<>();
    private static final Collection<String> POTENTIAL_PARENTS;
    static {
        CHILD_TO_PARENT.put("OBX", "OBR");
        POTENTIAL_PARENTS = CHILD_TO_PARENT.values();
    }

    /**
     * Parse an HL7 ORU^R01 message into segments.
     * @param messageAsStr The message as a string, (CR line ending)
     * @throws Hl7ParseException if it can't be parsed
     */
    public Hl7Message(String messageAsStr) throws Hl7ParseException {
        String[] segmentsAsStr = messageAsStr.split("\r");
        for (String seg: segmentsAsStr) {
            this.segments.add(new Hl7Segment(seg));
        }
        Map<String, Hl7Segment> mostRecentSegmentOfType = new HashMap<>();
        for (Hl7Segment seg : this.segments) {
            // keep track of the most recent (eg.) OBR
            if (POTENTIAL_PARENTS.contains(seg.getSegmentName())) {
                mostRecentSegmentOfType.put(seg.getSegmentName(), seg);
            }
            String parentSegmentName = CHILD_TO_PARENT.get(seg.getSegmentName());
            if (parentSegmentName != null) {
                Hl7Segment parentSegment = mostRecentSegmentOfType.get(parentSegmentName);
                if (parentSegment == null) {
                    throw new Hl7ParseException(String.format(
                            "Required parent %s for segment %s not found",
                            parentSegmentName, seg.getSegmentName()));
                }
                parentSegment.addChildSegment(seg);
                logger.debug("Adding segment {} to parent {}", seg.getSegmentName(), parentSegmentName);
            }
            logger.debug("Adding segment {}", seg.getSegmentName());
            List<Hl7Segment> existingSegments = this.segmentsBySegmentName.computeIfAbsent(
                    seg.getSegmentName(), k -> new ArrayList<>());
            existingSegments.add(seg);

        }
    }

    /**
     * Convenience function for segments where there is only one in the message.
     * Get the value of the given field as a string.
     * If you need to deal with multiple or nested segments (eg. OBR/OBX) you will have to use
     * Hl7Message#getSegments and/or Hl7Segment#getChildSegments.
     * @param segmentName name of the segment, eg. "MSH"
     * @param field1Index 1-indexed field as per HL7 spec
     * @return the whole field value as a string
     * @throws Hl7ParseException if there are != 1 of this segment in the message
     */
    public String getField(String segmentName, int field1Index) throws Hl7ParseException {
        Hl7Segment seg = getSingleHl7Segment(segmentName, field1Index);
        return seg.getField(field1Index);
    }

    private Hl7Segment getSingleHl7Segment(String segmentName, int field1Index) throws Hl7ParseException {
        List<Hl7Segment> seg = getSegments(segmentName);
        if (seg.size() != 1) {
            throw new Hl7ParseException(
                    String.format("getField(%s, %d) can only be called on single segments seg, got size %d",
                            segmentName, field1Index, seg.size()));
        }
        return seg.get(0);
    }

    public List<Hl7Segment> getSegments(String segmentName) {
        return segmentsBySegmentName.getOrDefault(segmentName, new ArrayList<>());
    }

}
