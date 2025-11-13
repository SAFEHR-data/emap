package uk.ac.ucl.rits.inform.informdb.visit_recordings;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;
import uk.ac.ucl.rits.inform.informdb.TemporalCore;
import uk.ac.ucl.rits.inform.informdb.annotation.AuditTable;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Index;
import javax.persistence.Inheritance;
import javax.persistence.InheritanceType;
import javax.persistence.Table;
import java.time.Instant;

/**
 * \brief VisitObservationType describes the meaning behind a specific observation.
 *
 * In EHR systems these are often coded either with potentially ambiguous short
 * names, or sometimes just numbers. This table maps these system level terms
 * into standardised vocabularies to make their meanings clear. While we currently use this table for flowsheets only,
 * we anticipate that we can use this table for smart data elements also.
 * @author Roma Klapaukh
 * @author Stef Piatek
 * @author Anika Cawthorn
 * @author Tom Young
 */
@Entity
@Data
@SuppressWarnings("serial")
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
@Inheritance(strategy = InheritanceType.TABLE_PER_CLASS)
@NoArgsConstructor
@AuditTable
@Table(indexes = {@Index(name = "vot_id_in_application", columnList = "idInApplication")})
public class VisitObservationType extends TemporalCore<VisitObservationType, VisitObservationTypeAudit> {

    /**
     * \brief Unique identifier in EMAP for this visitObservationTypeId record.
     *
     * This is the primary key for the visitObservationType table.
     */
    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    private long visitObservationTypeId;

    /**
     * \brief The ID used in the HL7 messages for referring to this visit observation type.
     *
     * This ID in conjunction with the application idInApplication and the sourceObservationType describes the visit observation type uniquely.
     */
    private String interfaceId;

    /**
     * \brief The data type in the source system.
     *
     * E.g. flowsheet or smart data elements
     */
    @Column(nullable = false)
    private String sourceObservationType;

    /**
     * \brief The code used by the source system to identify the observation type.
     *
     * The code used by the hospital application to identify the observation type. For flowsheets this is the flowsheetId retrieved
     * This ID in conjunction with the interfaceId uniquely identifies the
     * observation type.
     */
    private String idInApplication;

    /**
     * \brief Readable name for the source system observation type.
     */
    private String name;

    /**
     * \brief Name displayed to users.
     */
    private String displayName;

    /**
     * \brief Description of the data type.
     */
    private String description;

    /**
     * \brief Does this observation type update in real time?
     */
    private Boolean isRealTime;

    /**
     * \brief Does this observation type has an associated row in vist_observation?
     */
    private Boolean hasVisitObservation;

    /**
     * \brief Mapping code for the observation from the standardised vocabulary system. Not yet implemented.
     */
    private String standardisedCode;

    /**
     * \brief Nomenclature or classification system used. Not yet implemented.
     */
    private String standardisedVocabulary;

    /**
     * \brief Data type expected to be returned.
     */
    private String primaryDataType;

    /**
     * \brief Date and time at which this visitObservationType was created in the source system.
     */
    private Instant creationDatetime;

    /**
     * Minimal information constructor.
     * @param idInApplication       Hospital flowsheet identifier
     * @param interfaceId           Interface identifier
     * @param sourceObservationType Data type in the source system (e.g. flowsheet)
     * @param validFrom             When information last changed
     * @param storedFrom            When type of information was first processed in EMAP
     */
    public VisitObservationType(String idInApplication, String interfaceId, String sourceObservationType, Instant validFrom,
                                Instant storedFrom) {
        this.idInApplication = idInApplication;
        this.interfaceId = interfaceId;
        this.sourceObservationType = sourceObservationType;
        this.hasVisitObservation = false;
        this.isRealTime = false;
        setValidFrom(validFrom);
        setStoredFrom(storedFrom);
    }

    /**
     * Build new entity from existing one.
     * @param other existing entity
     */
    public VisitObservationType(VisitObservationType other) {
        super(other);
        visitObservationTypeId = other.visitObservationTypeId;
        sourceObservationType = other.sourceObservationType;
        idInApplication = other.idInApplication;
        interfaceId = other.interfaceId;
        name = other.name;
        displayName = other.displayName;
        description = other.description;
        standardisedCode = other.standardisedCode;
        standardisedVocabulary = other.standardisedVocabulary;
        primaryDataType = other.primaryDataType;
        creationDatetime = other.creationDatetime;
        isRealTime = other.isRealTime;
        hasVisitObservation = other.hasVisitObservation;
    }

    @Override
    public VisitObservationType copy() {
        return new VisitObservationType(this);
    }

    /**
     * @param validUntil  the event time that invalidated the current state
     * @param storedUntil the time that star started processing the message that invalidated the current state
     * @return A new audit entity with the current state of the object.
     */
    @Override
    public VisitObservationTypeAudit createAuditEntity(Instant validUntil, Instant storedUntil) {
        return new VisitObservationTypeAudit(this, validUntil, storedUntil);
    }
}
