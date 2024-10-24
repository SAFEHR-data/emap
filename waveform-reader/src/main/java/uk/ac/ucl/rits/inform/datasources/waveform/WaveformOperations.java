package uk.ac.ucl.rits.inform.datasources.waveform;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import uk.ac.ucl.rits.inform.interchange.messaging.Publisher;
import uk.ac.ucl.rits.inform.interchange.visit_observations.WaveformMessage;

@Component
public class WaveformOperations {
    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final Publisher publisher;

    public WaveformOperations(Publisher publisher) {
        this.publisher = publisher;
    }


    /**
     * Send message to rabbitmq.
     * @param msg the (collated) waveform message
     * @throws InterruptedException If the Publisher thread is interrupted
     */
    public void sendMessage(WaveformMessage msg) throws InterruptedException {
        if (msg.getSourceMessageId() == null || msg.getSourceMessageId().isEmpty()) {
            logger.error("ERROR: About to publish message with bad message ID {}", msg.getSourceMessageId());
        }
        String messageId = msg.getSourceMessageId();
        publisher.submit(msg, messageId, messageId, () -> {
            // XXX: If/when we find a way of re-requesting old messages, we may want to keep track of progress here
            // See issue #40.
            logger.debug("Successful ACK for message with ID {}", messageId);
        });
    }
}
