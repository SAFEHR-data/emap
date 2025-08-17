package uk.ac.ucl.rits.inform.datasinks.emapstar.repos;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Index;
import javax.persistence.Lob;
import javax.persistence.Table;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.time.Instant;

/**
 * What effect did each IDS message have on EMAP star db?
 * <p>
 * Useful for debugging, seeing why a particular message didn't have the
 * intended effect, etc
 * @author Jeremy Stein
 */
@Entity
@Table(name = "etl_per_message_logging",
        indexes = {@Index(columnList = "sourceId", unique = false)})
public class IdsEffectLogging {
    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    private Long id;
    private String sourceId;
    private Instant messageDatetime;
    private Instant processingStartTime;
    private Instant processingEndTime;
    /**
     * Time is takes to process the interchange message in nanoseconds.
     */
    private Long processMessageDuration;
    private Boolean error;
    private String messageType;
    @Lob
    private String message;
    @Lob
    private String stackTrace;

    /**
     * @param sourceId the unique ID from the source system (eg. IDS unid)
     */
    public void setSourceId(String sourceId) {
        this.sourceId = sourceId;
    }

    /**
     * @param processingStartTime when did processing this message start
     */
    public void setProcessingStartTime(Instant processingStartTime) {
        this.processingStartTime = processingStartTime;
    }

    /**
     * @param processingEndTime when did processing this message finish
     */
    public void setProcessingEndTime(Instant processingEndTime) {
        this.processingEndTime = processingEndTime;
    }

    /**
     * @param message text description of what action was taken based on this message
     */
    public void setMessage(String message) {
        this.message = message;
    }

    /**
     * @param messageType message type from IDS (aka trigger event)
     */
    public void setMessageType(String messageType) {
        this.messageType = messageType;
    }

    /**
     * @param messageDatetime the timestamp in the message itself
     */
    public void setMessageDatetime(Instant messageDatetime) {
        this.messageDatetime = messageDatetime;
    }

    /**
     * Was an error encountered in the processing of the message.
     * @param error true if an error was encountered
     */
    public void setError(Boolean error) {
        this.error = error;
    }

    /**
     * @param processDurationNanoSeconds how long it took to process the message
     */
    public void setProcessMessageDuration(Long processDurationNanoSeconds) {
        this.processMessageDuration = processDurationNanoSeconds;
    }

    /**
     * @param stackTrace stack trace text if you have one
     */
    public void setStackTrace(String stackTrace) {
        this.stackTrace = stackTrace;
    }

    /**
     * Convert stack trace from exception to text.
     * @param th throwable containing a stack trace
     */
    public void setStackTrace(Throwable th) {
        StringWriter st = new StringWriter();
        th.printStackTrace(new PrintWriter(st));
        setStackTrace(st.toString());
    }
}
