package uk.ac.ucl.rits.inform.datasources.ids;

import org.springframework.transaction.annotation.Transactional;

import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import java.time.Instant;

/**
 * Keep track of what message number we have processed up to.
 */
@Entity
@Table(name = "etl_ids_progress")
public class IdsProgress {
    @Id
    private int id;
    private int lastProcessedIdsUnid;
    private Instant lastProcessedMessageDatetime;
    private Instant lastProcessingDatetime;

    /**
     * Initialise. There is only ever one row, so it's always the same.
     */
    public IdsProgress() {
        id = 0;
        setLastProcessedIdsUnid(-1);
    }

    /**
     * @param lastProcessedIdsUnid the last processed message
     */
    public void setLastProcessedIdsUnid(int lastProcessedIdsUnid) {
        this.lastProcessedIdsUnid = lastProcessedIdsUnid;
    }

    /**
     * @return row id
     */
    public int getId() {
        return id;
    }

    /**
     * @return the last processed message
     */
    public int getLastProcessedIdsUnid() {
        return lastProcessedIdsUnid;
    }

    /**
     * @return the timestamp in the last processed message
     */
    public Instant getLastProcessedMessageDatetime() {
        return lastProcessedMessageDatetime;
    }

    /**
     * @param lastProcessedMessageDatetime the timestamp in the last processed message
     */
    public void setLastProcessedMessageDatetime(Instant lastProcessedMessageDatetime) {
        this.lastProcessedMessageDatetime = lastProcessedMessageDatetime;
    }

    /**
     * @return when the last processed message was processed
     */
    public Instant getLastProcessingDatetime() {
        return lastProcessingDatetime;
    }

    /**
     * @param lastProcessingDatetime when the last processed message was processed
     */
    public void setLastProcessingDatetime(Instant lastProcessingDatetime) {
        this.lastProcessingDatetime = lastProcessingDatetime;
    }

    /**
     * Record that we have processed all messages up to the specified message.
     * @param currentId             the unique ID for the latest IDS message processed
     * @param messageDatetime       the timestamp of this message
     * @param processingEnd         the time this message was actually processed
     * @param idsProgressRepository repository to save the entity
     */
    @Transactional
    void updateAndSave(int currentId, Instant messageDatetime, Instant processingEnd, IdsProgressRepository idsProgressRepository) {
        lastProcessedIdsUnid = currentId;
        lastProcessedMessageDatetime = messageDatetime;
        lastProcessingDatetime = processingEnd;
        idsProgressRepository.save(this);
    }
}
