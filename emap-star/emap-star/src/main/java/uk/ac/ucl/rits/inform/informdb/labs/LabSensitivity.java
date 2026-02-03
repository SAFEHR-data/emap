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
 * Sensitivities show the affect of specific agents on isolates from cultures.
 * @author Roma Klapaukh
 * @author Stef Piatek
 */
@SuppressWarnings("serial")
@Entity
@Data
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
@AuditTable
public class LabSensitivity extends TemporalCore<LabSensitivity, LabSensitivityAudit> {

    /**
     * \brief Unique identifier in EMAP for this labSensitivity record.
     *
     * This is the primary key for the labSensitivity table.
     */
    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    private long labSensitivityId;

    /**
     * \brief Identifier for the LabIsolate associated with this record.
     *
     * This is a foreign key that joins the labSensitivity table to the LabIsolate table.
     */
    @ManyToOne
    @JoinColumn(name = "labIsolateId", nullable = false)
    private LabIsolate labIsolateId;

    /**
     * \brief The chemical (often antibiotic) used.
     */
    private String agent;

    /**
     * \brief Sensitivity of the microbe to the agent.
     */
    private String sensitivity;

    /**
     * \brief Date and time at which this labSensitivity was reported.
     */
    private Instant reportingDatetime;

    public LabSensitivity() {}

    /**
     * Create minimal LabSensitivity.
     * @param labIsolateId parent LabIsolate
     * @param agent        antimicrobial agent
     */
    public LabSensitivity(LabIsolate labIsolateId, String agent) {
        this.labIsolateId = labIsolateId;
        this.agent = agent;
    }

    private LabSensitivity(LabSensitivity other) {
        super(other);
        this.labSensitivityId = other.labSensitivityId;
        this.labIsolateId = other.labIsolateId;
        this.agent = other.agent;
        this.sensitivity = other.sensitivity;
        this.reportingDatetime = other.reportingDatetime;
    }


    @Override
    public LabSensitivity copy() {
        return new LabSensitivity(this);
    }

    @Override
    public LabSensitivityAudit createAuditEntity(Instant validUntil, Instant storedUntil) {
        return new LabSensitivityAudit(this, validUntil, storedUntil);
    }
}
