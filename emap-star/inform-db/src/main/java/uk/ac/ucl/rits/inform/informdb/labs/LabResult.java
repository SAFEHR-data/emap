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
import javax.persistence.Index;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.Table;
import java.time.Instant;

/**
 * A LabResult is a single component result of a lab. A single order or sample
 * is likely to produce several results.
 * @author Roma Klapaukh
 * @author Stef Piatek
 */
@SuppressWarnings("serial")
@Entity
@Data
@Table(indexes = {@Index(name = "lr_lab_order_id", columnList = "labOrderId"),
        @Index(name = "lr_lab_test_definition_id", columnList = "labTestDefinitionId"),
        @Index(name = "lr_result_last_modified_time", columnList = "resultLastModifiedTime")})
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
@AuditTable
public class LabResult extends TemporalCore<LabResult, LabResultAudit> {

    /**
     * \brief Unique identifier in EMAP for this labResult record.
     *
     * This is the primary key for the labResult table.
     */
    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    private long labResultId;

    /**
     * \brief Identifier for the LabOrder associated with this record.
     *
     * This is a foreign key that joins the labResult table to the LabOrder table.
     */
    @ManyToOne
    @JoinColumn(name = "labOrderId", nullable = false)
    private LabOrder labOrderId;

    /**
     * \brief Identifier for the LabTestDefinition associated with this record.
     *
     * This is a foreign key that joins the labResult table to the LabTestDefinition table.
     */
    @ManyToOne
    @JoinColumn(name = "labTestDefinitionId", nullable = false)
    private LabTestDefinition labTestDefinitionId;

    /**
     * \brief Date and time at which the labResult was last modified.
     */
    @Column(columnDefinition = "timestamp with time zone")
    private Instant resultLastModifiedTime;

    /**
     * \brief Lab system flag for value outside of normal range.
     */
    private String abnormalFlag;

    /**
     * \brief Mime type (or custom type) of the value.
     *
     * This will tell you which column you should expect to be populated for the value.
     */
    private String mimeType;

    /**
     * \brief Value as text.
     */
    @Column(columnDefinition = "text")
    private String valueAsText;

    /**
     * \brief Value as a number.
     */
    private Double valueAsReal;

    /**
     * \brief Value as bytes.
     */
    @Column(columnDefinition = "bytea") //TODO should this be bytes not bytea
    private byte[] valueAsBytes;

    /**
     * \brief For numeric results, defines the operator used to define the value.
     *
     * For example an estimated GFR is given as `>90`, this would have a valueAsReal of `90` and a numeric operator of `>`
     * Most of the time, values are precise so have a `=` result operator
     */
    private String resultOperator;

    /**
     * \brief Upper limit of reference range.
     */
    private Double rangeHigh;

    /**
     * \brief Lower limit of reference range.
     */
    private Double rangeLow;

    /**
     * \brief Status of the result.
     */
    private String resultStatus;

    /**
     * \brief Units of the result.
     */
    private String units;

    /**
     * \brief Additional comments.
     */
    @Column(columnDefinition = "text")
    private String comment;

    public LabResult() {}

    /**
     * Create minimal LabResult.
     * @param labOrderId             parent LabOrder
     * @param labTestDefinitionId    LabTestDefinition of result
     * @param resultLastModifiedTime most recent update time of result
     */
    public LabResult(LabOrder labOrderId, LabTestDefinition labTestDefinitionId, Instant resultLastModifiedTime) {
        this.labOrderId = labOrderId;
        this.labTestDefinitionId = labTestDefinitionId;
        this.resultLastModifiedTime = resultLastModifiedTime;
    }


    private LabResult(LabResult other) {
        super(other);
        this.labResultId = other.labResultId;
        this.labOrderId = other.labOrderId;
        this.labTestDefinitionId = other.labTestDefinitionId;
        this.resultLastModifiedTime = other.resultLastModifiedTime;
        this.abnormalFlag = other.abnormalFlag;
        this.valueAsText = other.valueAsText;
        this.valueAsReal = other.valueAsReal;
        this.resultOperator = other.resultOperator;
        this.rangeHigh = other.rangeHigh;
        this.rangeLow = other.rangeLow;
        this.comment = other.comment;
        this.units = other.units;
        this.mimeType = other.mimeType;
        this.resultStatus = other.resultStatus;
        this.valueAsBytes = other.valueAsBytes;
    }

    @Override
    public LabResult copy() {
        return new LabResult(this);
    }

    @Override
    public LabResultAudit createAuditEntity(Instant validUntil, Instant storedUntil) {
        return new LabResultAudit(this, validUntil, storedUntil);
    }
}
