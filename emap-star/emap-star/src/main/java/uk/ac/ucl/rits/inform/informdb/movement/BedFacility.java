package uk.ac.ucl.rits.inform.informdb.movement;

import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.Table;
import java.io.Serializable;

/**
 * \brief Represents a facility available at a Bed.
 */
@SuppressWarnings("serial")
@Entity
@Table
@Data
@NoArgsConstructor
public class BedFacility implements Serializable {

    /**
     * \brief Unique identifier in EMAP for this bedFacility record.
     *
     * This is the primary key for the bedFacility table.
     */
    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "emap_id_sequence")
    private Long bedFacilityId;

    /**
     * \brief Identifier for the BedState associated with this record.
     *
     * This is a foreign key that joins the bedFacility table to the BedState table.
     */
    @ManyToOne
    @JoinColumn(name = "bedStateId", nullable = false)
    private BedState bedStateId;

    /**
     * \brief Type of facility available at bed.
     *
     * e.g. Cot, oxygen, Near Nurses Station...
     */
    @Column(nullable = false)
    private String type;


    /**
     * Create Bed Facility.
     * @param bedStateId parent bed state
     * @param type       type of facility
     */
    public BedFacility(BedState bedStateId, String type) {
        this.bedStateId = bedStateId;
        this.type = type;
    }
}
