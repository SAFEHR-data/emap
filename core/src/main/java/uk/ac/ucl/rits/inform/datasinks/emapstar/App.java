package uk.ac.ucl.rits.inform.datasinks.emapstar;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Profile;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.scheduling.annotation.EnableScheduling;
import uk.ac.ucl.rits.inform.datasinks.emapstar.exceptions.MessageIgnoredException;
import uk.ac.ucl.rits.inform.datasinks.emapstar.repos.IdsEffectLogging;
import uk.ac.ucl.rits.inform.datasinks.emapstar.repos.IdsEffectLoggingRepository;
import uk.ac.ucl.rits.inform.interchange.AdvanceDecisionMessage;
import uk.ac.ucl.rits.inform.interchange.EmapOperationMessage;
import uk.ac.ucl.rits.inform.interchange.EmapOperationMessageProcessingException;
import uk.ac.ucl.rits.inform.interchange.PatientConditionMessage;
import uk.ac.ucl.rits.inform.interchange.adt.AdtMessage;
import uk.ac.ucl.rits.inform.interchange.lab.LabOrderMsg;
import uk.ac.ucl.rits.inform.interchange.visit_observations.ObservationType;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;

/**
 * Entry point class for the HL7 pipeline.
 * @author Jeremy Stein
 */
@SpringBootApplication(scanBasePackages = {
        "uk.ac.ucl.rits.inform.datasinks",
        "uk.ac.ucl.rits.inform.informdb"})
@EnableCaching
@EnableScheduling
public class App {
    private static final Logger logger = LoggerFactory.getLogger(App.class);

    @Autowired
    private InformDbOperations dbOps;

    @Autowired
    private IdsEffectLoggingRepository idsEffectLoggingRepository;

    /**
     * Added this to get Instant objects (de)serialising properly.
     * @return our message converter
     */
    @Bean
    public static MessageConverter jsonReaderMessageConverter() {
        ObjectMapper mapper = new ObjectMapper().findAndRegisterModules();
        return new Jackson2JsonMessageConverter(mapper);
    }

    /**
     * @param args command line args
     */
    public static void main(String[] args) {
        SpringApplication.run(App.class, args);
    }


    /**
     * The listener for processing messages and writing to Emap-Star. The ordering
     * of queue names in the `queues` parameter actually matters - we want HL7
     * messages to be processed in preference to caboodle messages.
     * @param msg     the message
     * @param channel the rabbitmq channel
     * @param tag     the message tag
     * @throws IOException if rabbitmq channel has a problem
     */
    @Profile("default")
    @RabbitListener(queues = "#{'${core.rabbitmq.listen_queues}'.split(',')}")
    public void receiveMessage(EmapOperationMessage msg, Channel channel, @Header(AmqpHeaders.DELIVERY_TAG) long tag)
            throws IOException {
        IdsEffectLogging idsEffectLogging = new IdsEffectLogging();
        Instant startTime = Instant.now();
        idsEffectLogging.setProcessingStartTime(startTime);
        idsEffectLogging.setMessageType(msg.getMessageType());
        idsEffectLogging.setMessageDatetime(inferEventDatetime(msg));
        idsEffectLogging.setSourceId(msg.getSourceMessageId());
        try {
            logger.info("Starting processing of interchange message {}", msg.getSourceMessageId());
            logger.trace("{}", msg);
            msg.processMessage(dbOps);
            Instant doneProcessMessageTime = Instant.now();
            Duration processMessageDuration = Duration.between(startTime, doneProcessMessageTime);
            idsEffectLogging.setProcessMessageDuration(processMessageDuration.toNanos());
            idsEffectLogging.setError(false);
            logger.info("Sending ACK for {}", msg.getSourceMessageId());
            channel.basicAck(tag, false);
        } catch (EmapOperationMessageProcessingException e) {
            // All errors that allow the message to be skipped should be logged
            // using the return code from processMessage.
            // MessageIgnoredException is not an error, all others are
            idsEffectLogging.setError(!(e instanceof MessageIgnoredException));
            idsEffectLogging.setMessage(e.getMessage());
            idsEffectLogging.setStackTrace(e);
            logger.info("Sending NACK no requeue then NOT throwing for {}", msg.getSourceMessageId());
            channel.basicNack(tag, false, false);
        } catch (Throwable th) {
            // For anything else, at least log it before exiting.
            idsEffectLogging.setError(true);
            idsEffectLogging.setMessage(th.getMessage());
            idsEffectLogging.setStackTrace(th);
            logger.info("Sending NACK no requeue then throwing for {}", msg.getSourceMessageId());
            channel.basicNack(tag, false, false);
            throw th;
        } finally {
            idsEffectLogging.setProcessingEndTime(Instant.now());
            idsEffectLoggingRepository.save(idsEffectLogging);
        }
    }

    /**
     * Get an approximate idea of the message timestamp. Ideally we'd just use the persist datetime
     * on the IDS but we don't have that available to us here.
     * @param msg an interchagne message
     * @return when the event happened
     */
    private Instant inferEventDatetime(EmapOperationMessage msg) {
        // perhaps EmapOperationMessage should have an abstract method for getting the event datetime...
        if (msg instanceof AdtMessage) {
            return ((AdtMessage) msg).getRecordedDateTime();
        } else if (msg instanceof PatientConditionMessage) {
            return ((PatientConditionMessage) msg).getUpdatedDateTime();
        } else if (msg instanceof AdvanceDecisionMessage) {
            return ((AdvanceDecisionMessage) msg).getStatusChangeDatetime();
        } else if (msg instanceof ObservationType) {
            return ((ObservationType) msg).getLastUpdatedInstant();
        } else if (msg instanceof LabOrderMsg) {
            return ((LabOrderMsg) msg).getStatusChangeTime();
        }
        return null;
    }

}
