package uk.ac.ucl.rits.inform.datasources.waveform.hl7parse;

import lombok.Getter;
import lombok.Setter;

public class Hl7ParseException extends Exception {
    @Setter @Getter
    private String hl7Message;

    /**
     * HL7 parser error.
     * @param hl7Message original hl7 message
     * @param errorString description of error
     */
    public Hl7ParseException(String hl7Message, String errorString) {
        super(errorString);
        this.hl7Message = hl7Message;
    }
}
