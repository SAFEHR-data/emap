package uk.ac.ucl.rits.inform.interchange.messaging;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import uk.ac.ucl.rits.inform.interchange.EmapOperationMessage;
import uk.ac.ucl.rits.inform.interchange.springconfig.EmapDataSource;

import javax.annotation.PreDestroy;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;


/**
 * Publishes messages to rabbitmq, resending messages that receive a nack messaged.
 * @author Stef Piatek
 */
@Component
public class Publisher implements Runnable, Releasable {
    private final RabbitTemplate rabbitTemplate;
    private final Semaphore semaphore;
    private final BlockingQueue<MessageBatch<? extends EmapOperationMessage>> blockingQueue;
    private final Map<String, EmapOperationMessage> waitingMap;
    private final Map<String, ImmutablePair<Integer, Runnable>> batchWaitingMap;
    private final ScheduledThreadPoolExecutor executorService;
    private final int maxInTransit;
    private volatile boolean failedSend = false;
    private final int initialDelay;
    private int currentDelay;
    private int countMessagesAtCurrentDelay = 0;
    private final int delayMultiplier = 2;
    private @Value("${rabbitmq.retry.delay.maximum:600}")
    int maximumDelay;
    private final Thread mainThread;
    private volatile boolean isFinished;


    private final Logger logger = LoggerFactory.getLogger(Publisher.class);

    private final EmapDataSource getEmapDataSource;

    /**
     * @param rabbitTemplate rabbitTemplate bean
     * @param emapDataSource emapDataSource bean
     * @param maxBatches     Application properties value rabbitmq.max.batches
     *                       Sets the maximum number of batches allowed before blocking
     * @param maxInTransit   Application properties value rabbitmq.max.batches
     *                       Sets the maximum number of messages that can be awaiting an acknowledgement by rabbitmq
     * @param initialDelay   Initial delay for a failed message to be resent in seconds
     */
    @Autowired
    public Publisher(RabbitTemplate rabbitTemplate,
                     EmapDataSource emapDataSource,
                     @Value("${rabbitmq.max.batches:1}") int maxBatches,
                     @Value("${rabbitmq.max.intransit:1}") int maxInTransit,
                     @Value("${rabbitmq.retry.delay.initial:1}") int initialDelay) {
        semaphore = new Semaphore(maxInTransit, true);
        rabbitTemplate.setConfirmCallback(new MessagesConfirmCallback(this));
        this.rabbitTemplate = rabbitTemplate;
        this.getEmapDataSource = emapDataSource;
        blockingQueue = new ArrayBlockingQueue<>(maxBatches);
        waitingMap = new ConcurrentHashMap<>();
        batchWaitingMap = new ConcurrentHashMap<>();
        executorService = new ScheduledThreadPoolExecutor(2);
        this.maxInTransit = maxInTransit;
        this.initialDelay = initialDelay;
        currentDelay = initialDelay;
        isFinished = false;
        mainThread = new Thread(this);
        mainThread.start();
    }

    /**
     * Submit single message for publication to rabbitmq queue defined in the configs EmapDataSource bean.
     * @param message       Emap message to be sent.
     * @param correlationId Unique Id for the message. Must not contain a colon character.
     * @param batchId       Unique Id for the batch, in most cases this can be the correlationId.
     *                      Must not contain a colon character.
     * @param callback      To be run on receipt of a successful acknowledgement of publishing from rabbitmq.
     *                      Most likely to update the state of progress.
     * @throws InterruptedException  if thread gets interrupted during queue put wait
     * @throws IllegalStateException if publisher has been shut down
     */
    public void submit(EmapOperationMessage message, String correlationId, String batchId, Runnable callback)
            throws InterruptedException, IllegalStateException {
        ImmutablePair<EmapOperationMessage, String> pair = new ImmutablePair<>(message, correlationId);
        List<ImmutablePair<EmapOperationMessage, String>> list = new ArrayList<>();
        list.add(pair);
        submit(list, batchId, callback);
    }

    /**
     * Submit batch of messages for publication to rabbitmq queue defined in the configs EmapDataSource bean.
     * @param batch    Batch of messages to be sent (pairs of Emap messages and their unique correlationIds)
     *                 CorrelationIds should be unique within the batch and not contain a colon character.
     * @param batchId  Unique Id for the batch, in most cases this can be the first correlationId of the batch.
     *                 Must not contain a colon character.
     * @param callback To be run on receipt of a successful acknowledgement of publishing all messages in batch from rabbitmq
     *                 Most likely to update the state of progress
     * @param <T>      Any child of EmapOperationMessage so that you can pass in child class directly.
     * @throws InterruptedException  if thread gets interrupted during queue put wait
     * @throws IllegalStateException if publisher has been shut down
     */
    public <T extends EmapOperationMessage> void submit(List<ImmutablePair<T, String>> batch, String batchId, Runnable callback)
            throws InterruptedException, IllegalStateException {
        if (isFinished) {
            throw new IllegalStateException("Publisher has been shut down");
        }
        MessageBatch<T> submitBatch = new MessageBatch<>(batchId, batch, callback);

        // If queue is full for longer than the scan for new messages, then the progress would not have been updated
        // so check ensure that we're not adding a duplicate batchId of one in progress or a waiting batch
        if (batchWaitingMap.containsKey(batchId) || blockingQueue.contains(submitBatch)) {
            logger.error("Queue with a batchId of {} already exists", batchId);
            return;
        }
        try {
            blockingQueue.put(submitBatch);
        } catch (InterruptedException e) {
            logger.error("Waiting to submit a batch was interrupted", e);
            throw e;
        }
        logger.trace("BatchId {} with {} messages was submitted to Publisher batches", batchId, batch.size());
    }

    /**
     * Shutdown all threads managed by publisher, managed by spring.
     */
    @PreDestroy
    public void shutdown() {
        executorService.shutdownNow();
        isFinished = true;
        mainThread.interrupt();
    }

    /**
     * If the number of messages published to rabbitmq queue is less than the maximum number of elements in flight,
     * then the message will be added to the waitingMap and published to rabbitmq.
     * Otherwise the it will block here and keep on retrying until an acknowledgement of reciept from rabbitmq is received.
     * @param message       Emap message to be sent.
     * @param correlationId Unique Id for the message. Must not contain a colon character.
     * @param batchId       Unique Id for the batch, in most cases this can be the correlationId.
     *                      Must not contain a colon character.
     * @throws InterruptedException if thread is interrupted while waiting to acquire semaphore
     */
    private void publish(EmapOperationMessage message, String correlationId, String batchId) throws InterruptedException {
        logger.trace("Sending message to RabbitMQ");

        CorrelationData correlationData = new CorrelationData(correlationId + ":" + batchId);
        try {
            semaphore.acquire();
        } catch (InterruptedException e) {
            logger.error("Waiting to send message to rabbitmq was interrupted", e);
            throw e;
        }
        waitingMap.put(correlationId, message);
        rabbitTemplate.convertAndSend(getEmapDataSource.getQueueName(), message, correlationData);
    }

    /**
     * Takes batches of submitted messages from the blockingQueue, tracks the batch as waiting
     * and attempts to sequentially publish the messages in the queue to rabbitmq.
     */
    public void run() {
        while (!isFinished) {
            try {
                MessageBatch<? extends EmapOperationMessage> messageBatch = blockingQueue.take();
                batchWaitingMap.put(messageBatch.getBatchId(), new ImmutablePair<>(messageBatch.getNumberOfMessages(), messageBatch.getCallback()));
                for (ImmutablePair<? extends EmapOperationMessage, String> pair : messageBatch.getBatch()) {
                    publish(pair.getLeft(), pair.getRight(), messageBatch.getBatchId());
                }
            } catch (AmqpException e) {
                logger.error("AMQP Exception encountered, shutting down the publisher", e);
                shutdown();
            } catch (InterruptedException e) {
                logger.error("Publisher thread interrupted", e);
                return;
            }
        }
    }

    /**
     * On acknowledgement from rabbitmq allow new messages to be sent and run the batch's callback runnable
     * (most likely to update the progress). If the entire batch has been successfully finished, then allow space
     * for another batch to be added to the Publisher.
     * <p>
     * There are two possible states that this is called from:
     * - If there have been no nacks received: Free up a single space for a new message to be sent
     * - If there has been a nack and this is the first ack: Free up all spaces for new messages to be published again.
     * @param correlationId correlationId + ":" + batchId (within the correlationData sent to rabbitmq).
     */
    @Override
    public void finishedSending(String correlationId) {
        String[] ids = correlationId.split(":");
        if (ids.length != 2) {
            logger.error("Colon was present in correlationId or batchId provided");
            throw new IllegalStateException("Colon was present in correlationId or batchId provided");
        }

        // Synchronised to make sure that only one thread is releasing semaphores if there was a previous nack
        synchronized (waitingMap) {
            waitingMap.remove(ids[0]);
            if (failedSend) {
                currentDelay = initialDelay;
                if (waitingMap.isEmpty()) {
                    failedSend = false;
                    semaphore.release(maxInTransit);
                }
            } else {
                semaphore.release();
            }
        }
        synchronized (batchWaitingMap) {
            ImmutablePair<Integer, Runnable> batchState = batchWaitingMap.get(ids[1]);
            int countOfWaitingMessages = batchState.getLeft() - 1;
            if (countOfWaitingMessages == 0) {
                batchWaitingMap.remove(ids[1]);
                // Real work done in a separate thread so that it doesn't block the event thread
                executorService.execute(batchState.getRight());
            } else {
                batchWaitingMap.put(ids[1], new ImmutablePair<>(countOfWaitingMessages, batchState.getRight()));
            }
        }
        logger.trace("Sent message with correlationId: {}", correlationId);

    }

    /**
     * On a nack response, no new messages will be sent, attempting to resend the messages that have failed to publish.
     * <p>
     * Failed messages will be sent with an exponential backoff, using the 'rabbitmq.retry.delay.initial'
     * and the 'rabbitmq.retry.delay.maximum' from application.properties as the seconds delay. The exponential backoff
     * will double after every message in transit has received a nack.
     * @param correlationId correlationId + ":" + batchId (within the correlationData sent to rabbitmq).
     */
    @Override
    public void failedSending(final String correlationId) {
        // On first Nack, drain all permits so only retries will be republished
        semaphore.drainPermits();
        failedSend = true;

        String[] ids = correlationId.split(":");
        if (ids.length != 2) {
            logger.error("Colon was present in correlationId or batchId provided");
            throw new IllegalStateException("Colon was present in correlationId or batchId provided");
        }
        final EmapOperationMessage message = waitingMap.get(ids[0]);
        final CorrelationData correlationData = new CorrelationData(correlationId);

        executorService.schedule(new Runnable() {
            @Override
            public void run() {
                rabbitTemplate.convertAndSend(getEmapDataSource.getQueueName(), message, correlationData);
                String queueFull = "Failed to deliver message (correlationData {}) was resent after a delay of {} seconds";
                if (currentDelayIsFirstThreeRounds()) {
                    logger.trace(queueFull, correlationData, currentDelay);
                } else {
                    logger.info(queueFull, correlationData, currentDelay);
                }
            }
        }, currentDelay, TimeUnit.SECONDS);

        if (currentDelay < maximumDelay && countMessagesAtCurrentDelay == maxInTransit) {
            currentDelay *= delayMultiplier;
            countMessagesAtCurrentDelay = 1;
        } else if (currentDelay < maximumDelay) {
            countMessagesAtCurrentDelay += 1;
        } else {
            currentDelay = maximumDelay;
        }
    }

    /**
     * @return true if the current delay has been multiplied less than 3 times.
     */
    private boolean currentDelayIsFirstThreeRounds() {
        return currentDelay <= initialDelay * delayMultiplier * delayMultiplier;
    }
}
