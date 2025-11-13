package uk.ac.ucl.rits.inform.informdb.labs;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import uk.ac.ucl.rits.inform.informdb.TemporalCore;
import uk.ac.ucl.rits.inform.informdb.annotation.AuditTable;
import uk.ac.ucl.rits.inform.informdb.identity.Mrn;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Index;
import javax.persistence.JoinColumn;
import javax.persistence.Lob;
import javax.persistence.ManyToOne;
import javax.persistence.Table;
import java.time.Instant;

/**
 * A LabSample details the external lab's view of a sample being analysed and its receipt by the lab system.
 * @author Roma Klapaukh
 */
@SuppressWarnings("serial")
@Entity
@Data
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
@AuditTable
@Table(indexes = {@Index(name = "lo_mrn_id", columnList = "mrnId")})
public class LabSample extends TemporalCore<LabSample, LabSampleAudit> {

    /**
     * \brief Unique identifier in EMAP for this labSample record.
     *
     * This is the primary key for the labSample table.
     */
    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    private long labSampleId;

    /**
     * \brief Identifier for the Mrn associated with this record.
     *
     * This is a foreign key that joins the labSample table to the Mrn table.
     */
    @ManyToOne
    @JoinColumn(name = "mrnId", nullable = false)
    private Mrn mrnId;

    /**
     * \brief Lab number for the system doing the lab test.
     */
    private String externalLabNumber;

    /**
     * \brief Date and time at which this labSample arrived at the lab.
     *
     * where there test was being performed.
     */
    private Instant receiptAtLabDatetime;

    /**
     * \brief Date and time at which this labSample was take from the patient.
     *
     * (e.g. time of phlebotomy).
     */
    private Instant sampleCollectionDatetime;

    /**
     * \brief Type of specimen.
     *
     * E.g. Mid Stream Urine
     */
    private String specimenType;

    /**
     * \brief Site on body the sample was taken from.
     *
     * E.g. Right kidney
     */
    private String sampleSite;

    /**
     * \brief Method of collection
     *
     * E.g. Bone marrow trephine biopsy
     */
    @Lob
    private String collectionMethod;

    public LabSample() {}

    /**
     * Create minimal valid LabSample.
     * @param mrnId             Mrn for sample
     * @param externalLabNumber lab number for sample
     * @param validFrom         time that the message was valid from
     * @param storedFrom        time that emap core stared processing the message
     */
    public LabSample(Mrn mrnId, String externalLabNumber, Instant validFrom, Instant storedFrom) {
        this.mrnId = mrnId;
        this.externalLabNumber = externalLabNumber;
        setValidFrom(validFrom);
        setStoredFrom(storedFrom);
    }

    private LabSample(LabSample other) {
        super(other);
        this.mrnId = other.mrnId;
        this.labSampleId = other.labSampleId;
        this.externalLabNumber = other.externalLabNumber;
        this.receiptAtLabDatetime = other.receiptAtLabDatetime;
        this.sampleCollectionDatetime = other.sampleCollectionDatetime;
        this.specimenType = other.specimenType;
        this.sampleSite = other.sampleSite;
        this.collectionMethod = other.collectionMethod;
    }

    @Override
    public LabSample copy() {
        return new LabSample(this);
    }

    /**
     * @param validUntil  the event time that invalidated the current state
     * @param storedUntil the time that star started processing the message that invalidated the current state
     * @return A new audit entity with the current state of the object.
     */
    @Override
    public LabSampleAudit createAuditEntity(Instant validUntil, Instant storedUntil) {
        return new LabSampleAudit(this, validUntil, storedUntil);
    }
}
