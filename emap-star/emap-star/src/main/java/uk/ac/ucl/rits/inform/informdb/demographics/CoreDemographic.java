package uk.ac.ucl.rits.inform.informdb.demographics;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import uk.ac.ucl.rits.inform.informdb.TemporalCore;
import uk.ac.ucl.rits.inform.informdb.annotation.AuditTable;
import uk.ac.ucl.rits.inform.informdb.identity.Mrn;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Index;
import javax.persistence.Inheritance;
import javax.persistence.InheritanceType;
import javax.persistence.JoinColumn;
import javax.persistence.OneToOne;
import javax.persistence.Table;
import java.time.Instant;
import java.time.LocalDate;

/**
 * \brief A core demographic represents the main demographics stored around patients.
 *
 * These are attached to an MRN and describe patient level, rather than visit
 * level, data.
 * @author UCL RITS
 */
@SuppressWarnings("serial")
@Entity
@Data
@Table(indexes = {@Index(name = "cd_mrn_id", columnList = "mrnId")})
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
@Inheritance(strategy = InheritanceType.TABLE_PER_CLASS)
@AuditTable
public class CoreDemographic extends TemporalCore<CoreDemographic, CoreDemographicAudit> {

    /**
     * \brief Unique identifier in EMAP for this coreDemographic record.
     *
     * This is the primary key for the coreDemographics table.
     */
    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    private long coreDemographicId;

    /**
     * \brief Identifier for the Mrn associated with this record.
     *
     * This is a foreign key that joins the coreDemographic table to the Mrn table.
     */
    @OneToOne
    @JoinColumn(name = "mrnId", nullable = false)
    private Mrn mrnId;

    /**
     * \brief First name of the patient.
     */
    private String firstname;

    /**
     * \brief Middle name of the patient.
     */
    private String middlename;

    /**
     * \brief Last name of the patient.
     */
    private String lastname;

    /**
     * \brief Date of birth of the patient.
     */
    private LocalDate dateOfBirth;

    /**
     * \brief Date of death of the patient.
     */
    private LocalDate dateOfDeath;

    /**
     * \brief Date and time of birth of the patient.
     */
    private Instant datetimeOfBirth;

    /**
     * \brief Date and time of death of the patient.
     */
    private Instant datetimeOfDeath;

    /**
     * \brief Predicate determining whether the patient is alive.
     */
    private Boolean alive;

    /**
     * \brief Postcode of the patient's home address.
     */
    private String homePostcode;

    /**
     * \brief Sex of the patient.
     */
    private String sex;

    /**
     * \brief Ethnicity of the patient.
     */
    private String ethnicity;

    /**
     * Default constructor.
     */
    public CoreDemographic() {}

    /**
     * Construct with the Mrn.
     * @param mrnId MRN object.
     */
    public CoreDemographic(Mrn mrnId) {
        super();
        setMrnId(mrnId);
    }

    /**
     * Copy constructor.
     * @param other other demographic.
     */
    private CoreDemographic(CoreDemographic other) {
        super(other);
        this.coreDemographicId = other.coreDemographicId;
        this.mrnId = other.mrnId;
        this.firstname = other.firstname;
        this.middlename = other.middlename;
        this.lastname = other.lastname;

        this.dateOfBirth = other.dateOfBirth;
        this.dateOfDeath = other.dateOfDeath;

        this.datetimeOfBirth = other.datetimeOfBirth;
        this.datetimeOfDeath = other.datetimeOfDeath;

        this.alive = other.alive;
        this.homePostcode = other.homePostcode;
        this.sex = other.sex;
        this.ethnicity = other.ethnicity;
    }

    @Override
    public CoreDemographic copy() {
        return new CoreDemographic(this);
    }

    @Override
    public CoreDemographicAudit createAuditEntity(Instant validUntil, Instant storedUntil) {
        return new CoreDemographicAudit(this, validUntil, storedUntil);
    }

}
