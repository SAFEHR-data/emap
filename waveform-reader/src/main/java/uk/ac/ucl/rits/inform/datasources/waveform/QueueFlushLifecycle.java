package uk.ac.ucl.rits.inform.datasources.waveform;

import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;
import uk.ac.ucl.rits.inform.interchange.messaging.Publisher;

import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Delays Spring shutdown until the collator and publisher queues are flushed.
 * Ensures all queued messages are collated and sent before the application exits.
 * <p>
 * Exit code is stored in a static holder because it survives bean destruction,
 * so main() can explicitly exit with this code.
 */
@Component
public class QueueFlushLifecycle implements SmartLifecycle {
    private final Logger logger = LoggerFactory.getLogger(QueueFlushLifecycle.class);

    // Survives bean destruction; read by Application.main()
    @Getter
    private static AtomicInteger exitCode = new AtomicInteger(0);

    private final WaveformCollator waveformCollator;
    private final Publisher publisher;

    private volatile boolean running = false;

    /**
     * Bring together anything that might need flushing during shutdown.
     * @param waveformCollator
     * @param publisher
     */
    public QueueFlushLifecycle(WaveformCollator waveformCollator,
                               Publisher publisher) {
        this.waveformCollator = waveformCollator;
        this.publisher = publisher;
    }

    @Override
    public void start() {
        running = true;
    }

    @Override
    public void stop() {
        if (!running) {
            return;
        }
        logger.info("QueueFlushLifecycle: delaying shutdown until queues are flushed");
        try {
            waitForQueues(120 * 1000);
        } catch (TimeoutException e) {
            exitCode.set(2);
            logger.error("QueueFlushLifecycle: queue flush timed out, setting non-zero exit code");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.warn("Queue flush interrupted", e);
            exitCode.set(3);
        } catch (WaveformCollator.CollationException e) {
            logger.error("Collation error during flush", e);
            exitCode.set(4);
        } finally {
            running = false;
        }
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    /**
     * Passively wait until collator and publisher queues are drained.
     * @param timeoutMilliSeconds after how long to give up
     * @throws InterruptedException If the thread is interrupted
     * @throws WaveformCollator.CollationException if the data has a logical error that prevents collation
     * @throws TimeoutException if queues do not become drained in the specified time
     */
    public void waitForQueues(long timeoutMilliSeconds) throws InterruptedException, WaveformCollator.CollationException, TimeoutException {
        // monotonic clock
        long deadline = System.nanoTime() + timeoutMilliSeconds * 1_000_000;
        while (System.nanoTime() < deadline) {
            int collatorPending = waveformCollator.getPendingMessageCount();
            int publisherTotal = publisher.getTotalPendingMessageCount();

            if (collatorPending > 0 || publisherTotal > 0) {
                logger.info("Flushing: collator pending={}, publisher total pending={}",
                        collatorPending, publisherTotal);
            } else {
                logger.info("Queues flushed successfully");
                return;
            }

            Thread.sleep(500);
        }

        int collatorPending = waveformCollator.getPendingMessageCount();
        int publisherTotal = publisher.getTotalPendingMessageCount();
        logger.error("Flush timeout after {} ms: collator pending={}, publisher pending={}",
                timeoutMilliSeconds, collatorPending, publisherTotal);
        throw new TimeoutException();
    }
}
