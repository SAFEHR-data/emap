package uk.ac.ucl.rits.inform.informdb.movement;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import uk.ac.ucl.rits.inform.informdb.AuditCore;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.Table;
import java.time.Instant;


/**
 * \brief Represents the state of a given Room.
 */
@SuppressWarnings("serial")
@Entity
@Table
@Data
@NoArgsConstructor
@EqualsAndHashCode(callSuper = true)
public class RoomState extends AuditCore<RoomState> {

    /**
     * \brief Unique identifier in EMAP for this roomState record.
     *
     * This is the primary key for the roomState table.
     */
    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "emap_id_sequence")
    private Long roomStateId;

    /**
     * \brief Identifier for the Room associated with this record.
     *
     * This is a foreign key that joins the roomState table to the Room table.
     */
    @ManyToOne
    @JoinColumn(name = "roomId", nullable = false)
    private Room roomId;

    /**
     * \brief Indentifier ??
     */
    @Column(nullable = false, unique = true)
    private Long csn;

    /**
     * \brief Current status of the Room.
     */
    private String status;

    /**
     * \brief Predicate determining whether the Room is ready.
     */
    private Boolean isReady;

    /**
     * Constructor for an active room state.
     * @param roomId     room entity
     * @param csn        room CSN
     * @param status     current room status
     * @param isReady    is the room ready
     * @param validFrom  time that the room was first used as a contact
     * @param storedFrom time that star started processing the message
     */
    public RoomState(Room roomId, Long csn, String status, Boolean isReady, Instant validFrom, Instant storedFrom) {
        this.roomId = roomId;
        this.csn = csn;
        this.status = status;
        this.isReady = isReady;
        setValidFrom(validFrom);
        setStoredFrom(storedFrom);
    }

    private RoomState(RoomState other) {
        super(other);
        setValidFrom(other.getValidFrom());
        setStoredFrom(other.getStoredFrom());
        this.roomStateId = other.roomStateId;
        this.roomId = other.roomId;
        this.status = other.status;
        this.isReady = other.isReady;
        this.csn = other.csn;
    }

    @Override
    public RoomState copy() {
        return new RoomState(this);
    }

    /**
     * @param validUntil  the event time that invalidated the current state
     * @param storedUntil the time that star started processing the message that invalidated the current state
     * @return A new audit entity with the current state of the object.
     */
    @Override
    public RoomState createAuditEntity(Instant validUntil, Instant storedUntil) {
        RoomState audit = copy();
        audit.setValidUntil(validUntil);
        audit.setStoredUntil(storedUntil);
        return audit;
    }
}
