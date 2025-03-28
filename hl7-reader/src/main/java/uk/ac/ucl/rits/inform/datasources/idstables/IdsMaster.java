package uk.ac.ucl.rits.inform.datasources.idstables;

import java.time.Instant;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

/**
 * Representation of the main IDS table. Usually for reading but we can also
 * create a test IDS. This is outside of the package
 * uk.ac.ucl.rits.inform.datasources.ids, so won't be picked up during entity
 * scanning - this avoids creating an empty version of this table in the UDS.
 */
@Entity
@Table(name = "tbl_ids_master")
public class IdsMaster {
    @Id
    private int unid;
    private String patientname;
    private String patientmiddlename;
    private String patientsurname;
    private Instant dateofbirth;
    private String nhsnumber;
    private String hospitalnumber;
    private String patientclass;
    private String patientlocation;
    private Instant admissiondate;
    private Instant dischargedate;
    private String messagetype;
    private String senderapplication;
    private String messageidentifier;
    private String messageformat;
    private String messageversion;
    private Instant messagedatetime;
    @Column(columnDefinition = "text")
    private String hl7message;
    private Instant persistdatetime;

    /**
     * @return the IDS unique ID
     */
    public int getUnid() {
        return unid;
    }

    /**
     * @return patient name
     */
    public String getPatientname() {
        return patientname;
    }

    /**
     * @return patient middle name
     */
    public String getPatientmiddlename() {
        return patientmiddlename;
    }

    /**
     * @return patient surname
     */
    public String getPatientsurname() {
        return patientsurname;
    }

    /**
     * @return patient dob
     */
    public Instant getDateofbirth() {
        return dateofbirth;
    }

    /**
     * @return patient NHS number
     */
    public String getNhsnumber() {
        return nhsnumber;
    }

    /**
     * @return patient MRN
     */
    public String getHospitalnumber() {
        return hospitalnumber;
    }

    /**
     * @return patient class
     */
    public String getPatientclass() {
        return patientclass;
    }

    /**
     * @return patient location
     */
    public String getPatientlocation() {
        return patientlocation;
    }

    /**
     * @return patient admission date
     */
    public Instant getAdmissiondate() {
        return admissiondate;
    }

    /**
     * @return patient discharge date
     */
    public Instant getDischargedate() {
        return dischargedate;
    }

    /**
     * @return message type (aka trigger event)
     */
    public String getMessagetype() {
        return messagetype;
    }

    /**
     * @return sender application
     */
    public String getSenderapplication() {
        return senderapplication;
    }

    /**
     * @return message identifier
     */
    public String getMessageidentifier() {
        return messageidentifier;
    }

    /**
     * @return message format
     */
    public String getMessageformat() {
        return messageformat;
    }

    /**
     * @return message version
     */
    public String getMessageversion() {
        return messageversion;
    }

    /**
     * @return message timestamp
     */
    public Instant getMessagedatetime() {
        return messagedatetime;
    }

    /**
     * @return the HL7 message text
     */
    public String getHl7message() {
        return hl7message;
    }

    /**
     * @return timestamp the message was persisted to the IDS
     */
    public Instant getPersistdatetime() {
        return persistdatetime;
    }

    /**
     * @param hl7message the HL7 message text
     */
    public void setHl7message(String hl7message) {
        this.hl7message = hl7message;
    }

    /**
     * @param unid the IDS unique ID
     */
    public void setUnid(int unid) {
        this.unid = unid;
    }

    /**
     * @param messagetype message type (aka trigger event)
     */
    public void setMessagetype(String messagetype) {
        this.messagetype = messagetype;
    }

    /**
     * @param hospitalnumber patient MRN
     */
    public void setHospitalnumber(String hospitalnumber) {
        this.hospitalnumber = hospitalnumber;
    }

    /**
     * @param patientname patient name
     */
    public void setPatientname(String patientname) {
        this.patientname = patientname;
    }

    /**
     * @param patientmiddlename patient middle name
     */
    public void setPatientmiddlename(String patientmiddlename) {
        this.patientmiddlename = patientmiddlename;
    }

    /**
     * @param patientsurname patient surname
     */
    public void setPatientsurname(String patientsurname) {
        this.patientsurname = patientsurname;
    }

    /**
     * @param dateofbirth patient dob
     */
    public void setDateofbirth(Instant dateofbirth) {
        this.dateofbirth = dateofbirth;
    }

    /**
     * @param nhsnumber patient NHS number
     */
    public void setNhsnumber(String nhsnumber) {
        this.nhsnumber = nhsnumber;
    }

    /**
     * @param patientclass patient class
     */
    public void setPatientclass(String patientclass) {
        this.patientclass = patientclass;
    }

    /**
     * @param patientlocation patient location
     */
    public void setPatientlocation(String patientlocation) {
        this.patientlocation = patientlocation;
    }

    /**
     * @param admissiondate patient admission date
     */
    public void setAdmissiondate(Instant admissiondate) {
        this.admissiondate = admissiondate;
    }

    /**
     * @param dischargedate patient discharge date
     */
    public void setDischargedate(Instant dischargedate) {
        this.dischargedate = dischargedate;
    }

    /**
     * @param senderapplication sender application
     */
    public void setSenderapplication(String senderapplication) {
        this.senderapplication = senderapplication;
    }

    /**
     * @param messageidentifier message identifier
     */
    public void setMessageidentifier(String messageidentifier) {
        this.messageidentifier = messageidentifier;
    }

    /**
     * @param messageformat message format
     */
    public void setMessageformat(String messageformat) {
        this.messageformat = messageformat;
    }

    /**
     * @param messageversion message version
     */
    public void setMessageversion(String messageversion) {
        this.messageversion = messageversion;
    }

    /**
     * @param messagedatetime message timestamp
     */
    public void setMessagedatetime(Instant messagedatetime) {
        this.messagedatetime = messagedatetime;
    }

    /**
     * @param persistdatetime timestamp the message was persisted to the IDS
     */
    public void setPersistdatetime(Instant persistdatetime) {
        this.persistdatetime = persistdatetime;
    }
}
