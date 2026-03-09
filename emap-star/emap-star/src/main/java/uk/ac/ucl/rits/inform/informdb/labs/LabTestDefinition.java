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
 * \brief This represents the definition of a single lab test by a single provider.
 *
 * An
 * individual test may feature more than once in this table, where there is more
 * than one lab provider that supplies it.
 * @author Roma Klapaukh
 * @author Stef Piatek
 */
@SuppressWarnings("serial")
@Entity
@Data
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
@AuditTable
public class LabTestDefinition extends TemporalCore<LabTestDefinition, LabTestDefinitionAudit> {

    /**
     * \brief Unique identifier in EMAP for this labTestDefinition record.
     *
     * This is the primary key for the labTestDefinition table.
     */
    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "emap_id_sequence")
    private long labTestDefinitionId;

    /**
     * \brief What system this code belongs to. Examples could be WinPath, or Epic.
     */
    @Column(nullable = false)
    private String labProvider;

    /**
     * \brief The department within the lab responsible for the test.
     */
    private String labDepartment;

    /**
     * \brief The code for this test as reported by the lab.
     */
    @Column(nullable = false)
    private String testLabCode;

    /**
     * \brief The code for this test in a standardised vocabulary.
     */
    private String testStandardCode;

    /**
     * \brief Nomenclature or classification system used. Not yet implemented.
     */
    private String standardisedVocabulary;

    /**
     * \brief Human readable name of the lab test.
     */
    private String name;

    public LabTestDefinition() {}

    /**
     * Create minimal LabTestDefinition.
     * @param labProvider lab provider that has defined the lab code
     * @param testLabCode lab code
     */
    public LabTestDefinition(String labProvider, String testLabCode) {
        this.labProvider = labProvider;
        this.testLabCode = testLabCode;
    }

    private LabTestDefinition(LabTestDefinition other) {
        super(other);
        this.labTestDefinitionId = other.labTestDefinitionId;
        this.labProvider = other.labProvider;
        this.labDepartment = other.labDepartment;
        this.testLabCode = other.testLabCode;
        this.testStandardCode = other.testStandardCode;
        this.standardisedVocabulary = other.standardisedVocabulary;
        this.name = other.name;
    }

    @Override
    public LabTestDefinition copy() {
        return new LabTestDefinition(this);
    }

    @Override
    public LabTestDefinitionAudit createAuditEntity(Instant validUntil, Instant storedUntil) {
        return new LabTestDefinitionAudit(this, validUntil, storedUntil);
    }
}
