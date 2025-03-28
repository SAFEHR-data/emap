package uk.ac.ucl.rits.inform.informdb.labs;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import uk.ac.ucl.rits.inform.informdb.TemporalCore;
import uk.ac.ucl.rits.inform.informdb.annotation.AuditTable;
import uk.ac.ucl.rits.inform.informdb.identity.HospitalVisit;

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
 * \brief A LabOrder contains the details of the request to perform a lab investigation.
 *
 * A given LabSample may have multiple LabOrders. Each of the lab orders is defined by a unique combination of LabBattery and LabSample.
 * @author Roma Klapaukh
 * @author Stef Piatek
 */
@SuppressWarnings("serial")
@Entity
@Data
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
@AuditTable(indexes = {@Index(name = "loa_lab_sample_id", columnList = "labSampleId")})
@Table(indexes = {@Index(name = "lo_lab_battery_id", columnList = "labBatteryId"),
        @Index(name = "lo_lab_sample_id", columnList = "labSampleId"),
        @Index(name = "lo_hospital_visit_id", columnList = "hospitalVisitId"),
        @Index(name = "lo_order_datetime", columnList = "orderDatetime")})
public class LabOrder extends TemporalCore<LabOrder, LabOrderAudit> {

    /**
     * \brief Unique identifier in EMAP for this labOrder record.
     *
     * This is the primary key for the labOrder table.
     */
    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    private Long labOrderId;

    /**
     * \brief Identifier for the LabSample associated with this record.
     *
     * This is a foreign key that joins the labOrder table to the LabSample table.
     */
    @ManyToOne
    @JoinColumn(name = "labSampleId", nullable = false)
    private LabSample labSampleId;

    /**
     * \brief Identifier for the HospitalVisit associated with this record.
     *
     * This is a foreign key that joins the labOrder table to the HospitalVisit table.
     * Can have labs that are not linked to a hospital visit.
     */
    @ManyToOne
    @JoinColumn(name = "hospitalVisitId")
    private HospitalVisit hospitalVisitId;

    /**
     * \brief Identifier for the LabBattery associated with this record.
     *
     * This is a foreign key that joins the labOrder table to the LabBattery table.
     */
    @ManyToOne
    @JoinColumn(name = "labBatteryId", nullable = false)
    private LabBattery labBatteryId;

    /**
     * \brief Date and time at which this labOrder was actioned.
     */
    private Instant orderDatetime;

    /**
     * \brief Date and time at which this labOrder was requested.
     */
    private Instant requestDatetime;

    /**
     * \brief Additional information supplied.
     */
    @Lob
    private String clinicalInformation;

    /**
     * \brief Identifier used in source system for this labOrder.
     */
    private String internalLabNumber;

    /**
     * \brief Name of the source system where this labOrder was created.
     */
    private String sourceSystem;

    public LabOrder() {}

    /**
     * Create minimal LabOrder.
     * @param labBatteryId parent LabBattery
     * @param labSampleId  parent LabSample
     */
    public LabOrder(LabBattery labBatteryId, LabSample labSampleId) {
        this.labBatteryId = labBatteryId;
        this.labSampleId = labSampleId;
    }

    private LabOrder(LabOrder other) {
        super(other);
        this.labOrderId = other.labOrderId;
        this.labSampleId = other.labSampleId;
        this.labBatteryId = other.labBatteryId;
        this.hospitalVisitId = other.hospitalVisitId;
        this.orderDatetime = other.orderDatetime;
        this.requestDatetime = other.requestDatetime;
        this.clinicalInformation = other.clinicalInformation;
        this.internalLabNumber = other.internalLabNumber;
        this.sourceSystem = other.sourceSystem;
    }

    @Override
    public LabOrder copy() {
        return new LabOrder(this);
    }

    @Override
    public LabOrderAudit createAuditEntity(Instant validUntil, Instant storedUntil) {
        return new LabOrderAudit(this, validUntil, storedUntil);
    }
}
