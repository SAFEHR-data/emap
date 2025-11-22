package uk.ac.ucl.rits.inform.datasources.ids.hl7.parser;

import java.time.Instant;

import ca.uhn.hl7v2.HL7Exception;
import ca.uhn.hl7v2.model.v26.segment.MSH;
import ca.uhn.hl7v2.model.v26.segment.PID;
import ca.uhn.hl7v2.model.v26.segment.PV1;
import ca.uhn.hl7v2.model.v26.segment.TXA;

/**
 * Group together some common functionality that gets patient and visit info.
 * hence why it's up to the caller to find them.
 * @author Jeremy Stein
 * @author Stef Piatek
 */
public class NotesMetadataHl7 implements PV1Wrap, PIDWrap, MSHWrap, TXAWrap {
    private final MSH msh;
    private final PV1 pv1;
    private final PID pid;
    private final TXA txa;

    @Override
    public PV1 getPV1() {
        return pv1;
    }

    @Override
    public MSH getMSH() {
        return msh;
    }

    @Override
    public PID getPID() {
        return pid;
    }

    @Override
    public TXA getTXA() {
        return txa;
    }

    /**
     * Build the parser object from some basic segments.
     * @param msh the MSH segment from the message
     * @param pid the PID segment from the message
     * @param pv1 the PV1 segment from the message
     * @param txa the TXA segment from the message
     */
    public NotesMetadataHl7(MSH msh, PID pid, PV1 pv1, TXA txa) {
        this.msh = msh;
        this.pv1 = pv1;
        this.pid = pid;
        this.txa = txa;
    }

    /**
     * Get the visit number from the PV1 segment, falling back to the PID if that is empty.
     * @return visit number or an empty string if it is not defined
     * @throws HL7Exception if HAPI does
     */
    public String getVisitNumberFromPv1orPID() throws HL7Exception {
        String visitNumber = getVisitNumber();
        if (visitNumber.isEmpty()) {
            visitNumber = getPatientAccountNumber();
        }
        return visitNumber;
    }

    /**
     * Get the notes metadata originating instant from the MSH segment, falling back to the message timestamp if that is empty.
     * @return the notes metadata originating instant
     * @throws HL7Exception
     */
    public Instant getNotesMetadataOriginatingInstantFromMSHIfEmpty() throws HL7Exception {
        Instant originatingInstant = getNotesMetadataOriginatingInstant();
        if (originatingInstant == null) {
            originatingInstant = getMessageTimestamp();
        }
        return originatingInstant;
    }

    /**
     * Get the notes metadata editing instant from the TXA segment, falling back to the message timestamp if that is empty.
     * @return the notes metadata editing instant
     * @throws HL7Exception
     */
    public Instant getNotesMetadataEditingInstantFromTXAIfEmpty() throws HL7Exception {
        Instant editingInstant = getNotesMetadataActivityInstant();
        if (editingInstant == null) {
            editingInstant = getMessageTimestamp();
        }
        return editingInstant;
    }

}
