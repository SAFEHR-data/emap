package uk.ac.ucl.rits.inform.informdb.notes;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;
import uk.ac.ucl.rits.inform.informdb.TemporalCore;
import uk.ac.ucl.rits.inform.informdb.annotation.AuditTable;
import uk.ac.ucl.rits.inform.informdb.identity.HospitalVisit;

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
 * \brief Tracks the notes recorded on a patience record.
 * <p>
 * This doesn't record the actual note
 * @author Sarah Keating
 */
@Entity
@Data
@NoArgsConstructor
@ToString(callSuper = true)
@Table(indexes = {
        @Index(name = "nm_hospital_visit", columnList = "hospitalVisitId"),
        @Index(name = "nm_started_datetime", columnList = "startedDatetime"),
})
@AuditTable
public class NotesMetadata extends TemporalCore<NotesMetadata, NotesMetadataAudit> {
    /**
     * \brief Unique identifier in EMAP for this NotesMetadata record.
     * <p>
     * This is the primary key for the NotesMetadata table.
     */
    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    private Long notesMetadataId;

    /**
     * \brief Identifier used in source system for this notesMetadata.
     */
   @Column(nullable = false)
    private Long internalId;

    /**
     * \brief Identifier for the HospitalVisit associated with this record.
     * <p>
     * This is a foreign key that joins the locationVisit table to the HospitalVisit table.
     */
    @ManyToOne
    @JoinColumn(name = "hospitalVisitId", nullable = false)
    private HospitalVisit hospitalVisitId;

    /**
     * \brief The type of note (ADMIT, TRANSFER, DISCHARGE). to be decided
     */
    private String noteType;

    /**
     * \brief The date and time that the note was created.
     */
    @Column(columnDefinition = "timestamp with time zone")
    private Instant startedDatetime;

    /**
     * \brief The date and time that the note was last edited.
     */
    @Column(columnDefinition = "timestamp with time zone")
    private Instant lastEditDatetime;

    /**
     * \brief The role of the person creating/editing the note.
     */
    private String editorRole;

    /**
     * Minimal constructor.
     * @param internalId unique number for this record
     * @param hospitalVisitId associated hospital visit
     */
    public NotesMetadata(Long internalId, HospitalVisit hospitalVisitId) {
        this.hospitalVisitId = hospitalVisitId;
        this.internalId = internalId;
    }

    /**
     * Copy constructor.
     * @param other entity to copy
     */
    private NotesMetadata(NotesMetadata other) {
        super(other);
        internalId = other.internalId;
        notesMetadataId = other.notesMetadataId;
        hospitalVisitId = other.hospitalVisitId;
        noteType = other.noteType;
        editorRole = other.editorRole;
        startedDatetime = other.startedDatetime;
        lastEditDatetime = other.lastEditDatetime;
    }

    @Override
    public NotesMetadata copy() {
        return new NotesMetadata(this);
    }

    @Override
    public NotesMetadataAudit createAuditEntity(Instant validUntil, Instant storedUntil) {
        return new NotesMetadataAudit(this, validUntil, storedUntil);
    }
}
