package uk.ac.ucl.rits.inform.informdb.labs;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import uk.ac.ucl.rits.inform.informdb.TemporalCore;
import uk.ac.ucl.rits.inform.informdb.annotation.AuditTable;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import java.time.Instant;

/**
 * Isolates identified from culture.
 * @author Stef Piatek
 */
@SuppressWarnings("serial")
@Entity
@Data
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
@AuditTable
public class LabIsolate extends TemporalCore<LabIsolate, LabIsolateAudit> {

    /**
     * \brief Unique identifier in EMAP for this labIsolate record.
     *
     * This is the primary key for the labIsolate table.
     */
    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "emap_id_sequence")
    private long labIsolateId;

    /**
     * \brief Identifier for the LabResult associated with this record.
     *
     * This is a foreign key that joins the labIsolate table to the LabResult table.
     */
    @ManyToOne
    @JoinColumn(name = "labResultId", nullable = false)
    private LabResult labResultId;

    /**
     * \brief Internal Id of the labIsolate.
     *
     * This is constant throughout the results, where isolate code and name can change as more detail added.
     */
    private String labInternalId;

    /**
     * \brief Lab system's code for the isolate.
     */
    private String isolateCode;

    /**
     * \brief Name of the isolate.
     */
    private String isolateName;

    /**
     * \brief Method of culture.
     */
    private String cultureType;

    /**
     * \brief Usually CFU range, but can also be categorical.
     */
    private String quantity;

    /**
     * \brief Any clinical information for the isolate.
     *
     * E.g. Gentamicin resistant, or specific sub-species identification
     */
    private String clinicalInformation;


    public LabIsolate() {}

    /**
     * Create minimal LabIsolate.
     * @param labResultId   parent LabResult
     * @param labInternalId WinPath internal ID for sensitivity (per lab result)
     */
    public LabIsolate(LabResult labResultId, String labInternalId) {
        this.labResultId = labResultId;
        this.labInternalId = labInternalId;
    }


    private LabIsolate(LabIsolate other) {
        super(other);

        this.labIsolateId = other.labIsolateId;
        this.labResultId = other.labResultId;
        this.labInternalId = other.labInternalId;
        this.isolateCode = other.isolateCode;
        this.isolateName = other.isolateName;
        this.cultureType = other.cultureType;
        this.quantity = other.quantity;
        this.clinicalInformation = other.clinicalInformation;
    }


    @Override
    public LabIsolate copy() {
        return new LabIsolate(this);
    }

    @Override
    public LabIsolateAudit createAuditEntity(Instant validUntil, Instant storedUntil) {
        return new LabIsolateAudit(this, validUntil, storedUntil);
    }
}
