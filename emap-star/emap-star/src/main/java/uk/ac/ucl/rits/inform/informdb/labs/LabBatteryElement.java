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
 * \brief This represents all the different batteries of test that can be ordered
 *
 * what we know about which constituent tests make them up.
 * @author Roma Klapaukh
 * @author Stef Piatek
 */
@SuppressWarnings("serial")
@Entity
@Data
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
@AuditTable
public class LabBatteryElement extends TemporalCore<LabBatteryElement, LabBatteryElementAudit> {

    /**
     * \brief Unique identifier in EMAP for this labBatteryElement record.
     *
     * This is the primary key for the labBatteryElement table.
     */
    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "emap_id_sequence")
    private long labBatteryElementId;

    /**
     * \brief Identifier for the LabBattery associated with this record.
     *
     * This is a foreign key that joins the labBatteryElement table to the LabBattery table.
     */
    @ManyToOne
    @JoinColumn(name = "labBatteryId", nullable = false)
    private LabBattery labBatteryId;

    /**
     * \brief Identifier for the LabTestDefinition associated with this record.
     *
     * This is a foreign key that joins the labBatteryElement table to the LabTestDefinition table.
     */
    @ManyToOne
    @JoinColumn(name = "labTestDefinitionId", nullable = false)
    private LabTestDefinition labTestDefinitionId;


    public LabBatteryElement() {}

    /**
     * Create a valid LabBatteryElement.
     * @param labTestDefinitionId parent lab test definition
     * @param labBatteryId        parent lab battery id
     * @param validFrom           time that the message was valid from
     * @param storedFrom          time that emap core stared processing the message
     */
    public LabBatteryElement(LabTestDefinition labTestDefinitionId, LabBattery labBatteryId, Instant validFrom, Instant storedFrom) {
        this.labTestDefinitionId = labTestDefinitionId;
        this.labBatteryId = labBatteryId;
        setValidFrom(validFrom);
        setStoredFrom(storedFrom);
    }

    private LabBatteryElement(LabBatteryElement other) {
        super(other);
        this.labBatteryElementId = other.labBatteryElementId;
        this.labBatteryId = other.labBatteryId;
        this.labTestDefinitionId = other.labTestDefinitionId;
    }

    @Override
    public LabBatteryElement copy() {
        return new LabBatteryElement(this);
    }

    @Override
    public LabBatteryElementAudit createAuditEntity(Instant validUntil, Instant storedUntil) {
        return new LabBatteryElementAudit(this, validUntil, storedUntil);
    }

}
