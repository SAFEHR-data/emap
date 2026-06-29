package uk.ac.ucl.rits.inform.informdb.consults;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import uk.ac.ucl.rits.inform.informdb.TemporalCore;
import uk.ac.ucl.rits.inform.informdb.annotation.AuditTable;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import java.time.Instant;

/**
 * \brief Type of a ConsultationRequest made for a patient.
 *
 * Inpatient referrals and inpatient referrals ancillary departments.
 * The data model suggests consultation requests from pharmacy but it looks like these are not created.
 * @author Anika Cawthorn
 */
@Entity
@Data
@EqualsAndHashCode(callSuper = true)
@NoArgsConstructor
@AuditTable
public class ConsultationType extends TemporalCore<ConsultationType, ConsultationTypeAudit> {

    /**
     * \brief Unique identifier in EMAP for this consultationType record.
     *
     * This is the primary key for the consultationType table.
     */
    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "emap_id_sequence")
    private long consultationTypeId;
    @Column(nullable = false, unique = true)

    /**
     * \brief Code used in source system for this consultationType.
     */
    private String code;

    /**
     * \brief Human readable name for this consultationType.
     */
    private String name;

    /**
     * Minimal constructor to create ConsultRequestType and add name for it. As there's only one field for now, it's
     * also the only constructor required for now.
     * @param code       Consultation type as provided in ConsultationRequest message
     * @param validFrom  From which point in time the ConsultationType is valid
     * @param storedFrom Time point at which ConsultationType was stored first
     */
    public ConsultationType(String code, Instant validFrom, Instant storedFrom) {
        this.code = code;
        setValidFrom(validFrom);
        setStoredFrom(storedFrom);
    }

    /**
     * Build a new ConsultationType from an existing one.
     * @param other existing ConsultationType
     */
    public ConsultationType(ConsultationType other) {
        super(other);
        this.consultationTypeId = other.consultationTypeId;
        this.code = other.code;
        this.name = other.name;
    }

    @Override
    public ConsultationType copy() {
        return new ConsultationType(this);
    }

    @Override
    public ConsultationTypeAudit createAuditEntity(Instant validUntil, Instant storedUntil) {
        return new ConsultationTypeAudit(this, validUntil, storedUntil);
    }
}
