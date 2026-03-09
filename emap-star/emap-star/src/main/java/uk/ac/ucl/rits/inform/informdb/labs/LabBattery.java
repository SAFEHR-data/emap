package uk.ac.ucl.rits.inform.informdb.labs;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import uk.ac.ucl.rits.inform.informdb.TemporalCore;
import uk.ac.ucl.rits.inform.informdb.annotation.AuditTable;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import java.time.Instant;

/**
 * This represents all the different batteries of test that can be ordered.
 * @author Stef Piatek
 */
@SuppressWarnings("serial")
@Entity
@Data
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
@AuditTable
public class LabBattery extends TemporalCore<LabBattery, LabBatteryAudit> {

    /**
     * \brief Unique identifier in EMAP for this labBattery record.
     *
     * This is the primary key for the labBattery table.
     */
    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "emap_id_sequence")
    private long labBatteryId;

    /**
     * \brief Code for this battery of tests.
     */
    private String batteryCode;

    /**
     * \brief Human readable name for this battery of tests.
     */
    private String batteryName;

    /**
     * \brief Source system of this batteryCode.
     *
     * What system this code belongs to. Examples could be WinPath, or Epic.
     */
    @Column(nullable = false)
    private String labProvider;

    public LabBattery() {}

    /**
     * Create a valid Lab battery.
     * @param batteryCode battery code
     * @param labProvider lab provider using the code
     * @param validFrom   time that the message was valid from
     * @param storedFrom  time that emap core stared processing the message
     */
    public LabBattery(String batteryCode, String labProvider, Instant validFrom, Instant storedFrom) {
        this.batteryCode = batteryCode;
        this.labProvider = labProvider;
        setValidFrom(validFrom);
        setStoredFrom(storedFrom);
    }

    private LabBattery(LabBattery other) {
        super(other);
        this.labBatteryId = other.labBatteryId;
        this.batteryCode = other.batteryCode;
        this.batteryName = other.batteryName;
        this.labProvider = other.labProvider;
    }

    @Override
    public LabBattery copy() {
        return new LabBattery(this);
    }

    @Override
    public LabBatteryAudit createAuditEntity(Instant validUntil, Instant storedUntil) {
        return new LabBatteryAudit(this, validUntil, storedUntil);
    }

}
