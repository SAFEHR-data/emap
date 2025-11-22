package uk.ac.ucl.rits.inform.datasources.ids.hl7.parser;

import java.time.Instant;

import ca.uhn.hl7v2.HL7Exception;
import ca.uhn.hl7v2.model.v26.segment.TXA;
import uk.ac.ucl.rits.inform.datasources.ids.HL7Utils;

/**
 * Wrapper around the HAPI parser's TXA segment object, to make it easier to use.
 * Other methods could be added: see https://hapifhir.github.io/hapi-hl7v2/v27/apidocs/ca/uhn/hl7v2/model/v27/segment/TXA.html
 * e.g. TXA-2 Document type
 */
interface TXAWrap {
    /**
     * How to get the TXA segment.
     * @return the TXA segment
     */
    TXA getTXA();

    /**
     * @return TXA-2 the document type/or what I have called the noteType
     * @throws HL7Exception if HAPI does
     */
    default String getNotesMetadataTypeString() throws HL7Exception {
        return getTXA().getDocumentType().toString();
    }

    /**
     * @return TXA-6 Originating date time
     * @throws HL7Exception if HAPI does
     */
    default Instant getNotesMetadataOriginatingInstant() throws HL7Exception {
        return HL7Utils.interpretLocalTime(getTXA().getOriginationDateTime());
    }

    /**
     * @return TXA-4 activity date time
     * @throws HL7Exception if HAPI does
     */
    default Instant getNotesMetadataActivityInstant() throws HL7Exception {
        return HL7Utils.interpretLocalTime(getTXA().getActivityDateTime());
    }

 }
