package uk.ac.ucl.rits.inform.informdb.demographics;

import lombok.Data;
import lombok.EqualsAndHashCode;
import uk.ac.ucl.rits.inform.informdb.AuditCore;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Table;
import java.time.Instant;

@Entity
@Table
@Data
@EqualsAndHashCode(callSuper = true)
public class AuditCoreDemographic extends CoreDemographicParent implements AuditCore<CoreDemographicParent> {
    private static final long serialVersionUID = -8516988957488992519L;
    @Column
    private long originalEntityId;
    @Column(columnDefinition = "timestamp with time zone")
    private Instant validUntil;
    @Column(columnDefinition = "timestamp with time zone")
    private Instant storedUntil;


    /**
     * Default constructor.
     */
    public AuditCoreDemographic() {
    }

    /**
     * Constructor from original entity and invalidation times.
     * @param originalEntity original entity to be audited.
     * @param storedUntil    the time that this change is being made in the DB
     * @param validUntil     the time at which this fact stopped being true,
     *                       can be any amount of time in the past
     */
    public AuditCoreDemographic(final CoreDemographic originalEntity, final Instant validUntil, final Instant storedUntil) {
        super(originalEntity);
        this.validUntil = validUntil;
        this.storedUntil = storedUntil;
        this.originalEntityId = originalEntity.getCoreDemographicId();
    }
}
