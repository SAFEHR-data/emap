package uk.ac.ucl.rits.inform.informdb;

import java.sql.Timestamp;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;

@Entity
public class Encounter {

    @Id
    private int encounter_id;
    
    private int account;
    private int encounter;
    private Timestamp store_datetime;
    private Timestamp end_datetime;
    private String source_system;
    private Timestamp event_time;
    public int getEncounter_id() {
        return encounter_id;
    }
    public void setEncounter_id(int encounter_id) {
        this.encounter_id = encounter_id;
    }
    public int getAccount() {
        return account;
    }
    public void setAccount(int account) {
        this.account = account;
    }
    public int getEncounter() {
        return encounter;
    }
    public void setEncounter(int encounter) {
        this.encounter = encounter;
    }
    public Timestamp getStore_datetime() {
        return store_datetime;
    }
    public void setStore_datetime(Timestamp store_datetime) {
        this.store_datetime = store_datetime;
    }
    public Timestamp getEnd_datetime() {
        return end_datetime;
    }
    public void setEnd_datetime(Timestamp end_datetime) {
        this.end_datetime = end_datetime;
    }
    public String getSource_system() {
        return source_system;
    }
    public void setSource_system(String source_system) {
        this.source_system = source_system;
    }
    public Timestamp getEvent_time() {
        return event_time;
    }
    public void setEvent_time(Timestamp event_time) {
        this.event_time = event_time;
    }
    
}
