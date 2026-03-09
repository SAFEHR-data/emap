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
 * \brief Represents a room in the hospital.
 */
@SuppressWarnings("serial")
@Entity
@Table
@Data
@NoArgsConstructor
public class Room implements Serializable {

    /**
     * \brief Unique identifier in EMAP for this room record.
     *
     * This is the primary key for the room table.
     */
    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "emap_id_sequence")
    private Long roomId;

    /**
     * \brief Identifier for the Department associated with this record.
     *
     * This is a foreign key that joins the room table to the Department table.
     */
    @ManyToOne
    @JoinColumn(name = "departmentId", nullable = false)
    private Department departmentId;

    /**
     * \brief Text name used by HL7 for this room.
     */
    @Column(nullable = false)
    private String hl7String;


    /**
     * \brief Name for this room.
     */
    private String name;

    /**
     * Create new Room.
     * @param hl7String    hl7 string for room
     * @param name         name for room
     * @param departmentId parent Department
     */
    public Room(String hl7String, String name, Department departmentId) {
        this.hl7String = hl7String;
        this.name = name;
        this.departmentId = departmentId;
    }
}
