package uk.ac.ucl.rits.inform.informdb.movement;

import java.io.Serializable;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.Table;

import lombok.Data;

/**
 * \breif Known locations within the hospital.
 * @author UCL RITS
 */
@SuppressWarnings("serial")
@Entity
@Table
@Data
public class Location implements  Serializable {

    /**
     * \brief Unique identifier in EMAP for this location record.
     *
     * This is the primary key for the location table.
     */
    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "emap_id_sequence")
    private long locationId;

    /**
     * \brief Text value of the location.
     */
    @Column(nullable = false)
    private String locationString;

    /**
     * \brief Identifier for the Department associated with this record.
     *
     * This is a foreign key that joins the location table to the Department table.
     */
    @ManyToOne
    @JoinColumn(name = "departmentId")
    private Department departmentId;

    /**
     * \brief Identifier for the Room associated with this record.
     *
     * This is a foreign key that joins the location table to the Room table.
     */
    @ManyToOne
    @JoinColumn(name = "roomId")
    private Room roomId;

    /**
     * \brief Identifier for the Bed associated with this record.
     *
     * This is a foreign key that joins the location table to the Bed table.
     */
    @ManyToOne
    @JoinColumn(name = "bedId")
    private Bed bedId;

    public Location() {
    }

    public Location(String locationString) {
        this.locationString = locationString;
    }

    @Override
    public String toString() {
        return String.format("Location{locationId=%d, locationString='%s'}", locationId, locationString);
    }
}
