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
 * \brief Represents a bed in the hospital.
 */
 @SuppressWarnings("serial")
@Entity
@Table
@Data
@NoArgsConstructor
public class Bed implements Serializable {

    /**
     * \brief Unique identifier in EMAP for this bed record.
     *
     * This is the primary key for the bed table.
     */
    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "emap_id_sequence")
    private Long bedId;

    /**
     * \brief Identifier for the Room associated with this record.
     *
     * This is a foreign key that joins the bed table to the Room table.
     */
    @ManyToOne
    @JoinColumn(name = "roomId", nullable = false)
    private Room roomId;

    /**
     * \brief Text name used by HL7 for this bed.
     */
    @Column(nullable = false)
    private String hl7String;

    /**
     * Create new bed.
     * @param hl7String hl7 string for bed
     * @param roomId room id
     */
    public Bed(String hl7String, Room roomId) {
        this.hl7String = hl7String;
        this.roomId = roomId;
    }
}
