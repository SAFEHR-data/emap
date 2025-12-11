package uk.ac.ucl.rits.inform.datasources.waveform;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.integration.channel.MessagePublishingErrorHandler;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.dsl.MessageChannels;
import org.springframework.integration.dsl.Pollers;
import org.springframework.integration.ip.tcp.TcpReceivingChannelAdapter;
import org.springframework.integration.ip.tcp.connection.DefaultTcpNetConnectionSupport;
import org.springframework.integration.ip.tcp.connection.TcpNetConnection;
import org.springframework.integration.ip.tcp.connection.TcpNetServerConnectionFactory;
import org.springframework.integration.ip.tcp.serializer.ByteArraySingleTerminatorSerializer;
import org.springframework.integration.util.ErrorHandlingTaskExecutor;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHeaders;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.util.ErrorHandler;
import uk.ac.ucl.rits.inform.datasources.waveform.hl7parse.Hl7ParseException;

import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;

/**
 * Listen on a TCP port for incoming HL7 messages.
 */
@Configuration
public class Hl7ListenerConfig {
    // number of thread pools (of size 1) to perform file operations.
    // Stickiness to a thread pool based on the file itself (bedid + timestamp)
    private static final int HANDLER_PARTITIONS = 4;

    private final Logger logger = LoggerFactory.getLogger(Hl7ListenerConfig.class);

    private final ConfigurableListableBeanFactory beanFactory;
    private final Hl7ParseAndQueue hl7ParseAndQueue;
    private final AtomicBoolean contextShuttingDown = new AtomicBoolean(false);
    private final ErrorHandler hl7ExecutorErrorHandler;
    private static final String PARTIAL_PARSED_HEADER_KEY = "partiallyParsed";

    /**
     * Need to register some beans manually since their number is configurable.
     * @param beanFactory for registering beans
     * @param hl7ParseAndQueue parsing and queuing
     */
    public Hl7ListenerConfig(ConfigurableListableBeanFactory beanFactory, Hl7ParseAndQueue hl7ParseAndQueue) {
        this.beanFactory = beanFactory;
        this.hl7ParseAndQueue = hl7ParseAndQueue;
        MessagePublishingErrorHandler delegateErrorHandler = new MessagePublishingErrorHandler();
        delegateErrorHandler.setBeanFactory(beanFactory);
        this.hl7ExecutorErrorHandler = error -> {
            if (contextShuttingDown.get()) {
                logger.warn("Suppressed error during shutdown", error);
            } else {
                delegateErrorHandler.handleError(error);
            }
        };
        registerHl7HandlerExecutors();
    }

    @Bean
    ThreadPoolTaskExecutor hl7TcpListenTaskExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(4);
        executor.setMaxPoolSize(8);
        executor.setThreadNamePrefix("TcpListen-");
        executor.setQueueCapacity(20);
        executor.setWaitForTasksToCompleteOnShutdown(true);
        executor.setAwaitTerminationSeconds(600);
        executor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
        executor.initialize();
        return executor;
    }

    /**
     * Specify the server config.
     * @param listenPort port to listen on (inside container)
     * @param sourceAddressAllowList list of source addresses that are allowed to connect to us
     * @param hl7TcpListenTaskExecutor task executor to use for TCP listener
     * @return connection factory
     */
    @Bean
    public TcpNetServerConnectionFactory serverConnectionFactory(
            @Value("${waveform.hl7.listen_port}") int listenPort,
            @Value("${waveform.hl7.source_address_allow_list}") List<String> sourceAddressAllowList,
            ThreadPoolTaskExecutor hl7TcpListenTaskExecutor
    ) {
        TcpNetServerConnectionFactory connFactory = new TcpNetServerConnectionFactory(listenPort);
        connFactory.setSoSendBufferSize(1 * 1024 * 1024);
        connFactory.setSoReceiveBufferSize(1 * 1024 * 1024);
        connFactory.setTaskExecutor(hl7TcpListenTaskExecutor);
        connFactory.setSoTimeout(10_000);
        connFactory.setSoTcpNoDelay(false);
        connFactory.setSoKeepAlive(true);
        // The message separator is actually "\r\x1c\r\x0b", but there is no pre-existing
        // serializer which supports string separators.
        // Since the 0x1c (file separator) character is pretty unusual and only occurs here,
        // use this as a single byte separator and then we'll have to strip off the other junk later.
        // Spring will get upset if we get sent anything after this character. May need to squash this
        // error, at least if it's just some extraneous whitespace.
        ByteArraySingleTerminatorSerializer serializer = new ByteArraySingleTerminatorSerializer((byte) 0x1c);
        serializer.setMaxMessageSize(5_000_000);
        connFactory.setDeserializer(serializer);
        connFactory.setTcpNetConnectionSupport(new DefaultTcpNetConnectionSupport() {
            @Override
            public TcpNetConnection createNewConnection(
                    Socket socket,
                    boolean server,
                    boolean lookupHost,
                    ApplicationEventPublisher applicationEventPublisher,
                    String connectionFactoryName) {
                TcpNetConnection conn = super.createNewConnection(socket, server, lookupHost, applicationEventPublisher, connectionFactoryName);
                String sourceAddress = conn.getHostAddress();
                if (sourceAddressAllowList.contains(sourceAddress)
                        || sourceAddressAllowList.contains("ALL")) {
                    logger.info("connection accepted from {}:{}", sourceAddress, conn.getPort());
                } else {
                    logger.warn("CONNECTION REFUSED from {}:{}, allowlist = {}", sourceAddress, conn.getPort(), sourceAddressAllowList);
                    conn.close();
                }
                return conn;
            }
        });
        return connFactory;
    }

    /**
     * Routes the TCP connection to the message handling.
     * @param connectionFactory connection factory
     * @param hl7MessageChannel message channel for (split) HL7 messages
     * @return adapter
     */
    @Bean
    TcpReceivingChannelAdapter hl7InboundTcpAdapter(TcpNetServerConnectionFactory connectionFactory, MessageChannel hl7MessageChannel) {
        TcpReceivingChannelAdapter adapter = new TcpReceivingChannelAdapter();
        adapter.setConnectionFactory(connectionFactory);
        adapter.setOutputChannel(hl7MessageChannel);
        // Shutdown happens from high to low phase number,
        // so make sure the TCP listener is stopped before everything else so no new
        // messages come in while we're trying to drain the queues.
        // Default value (for a TcpReceivingChannelAdapter?) is 2^30-1, so set it higher
        // to be sure.
        adapter.setPhase(Integer.MAX_VALUE);
        return adapter;
    }

    @Bean
    ThreadPoolTaskScheduler pollerTaskScheduler() {
        ThreadPoolTaskScheduler threadPoolTaskScheduler = new ThreadPoolTaskScheduler();
        threadPoolTaskScheduler.setPoolSize(1);
        threadPoolTaskScheduler.setThreadNamePrefix("HL7QueuePoller-");
        threadPoolTaskScheduler.initialize();
        return threadPoolTaskScheduler;
    }

    @Bean
    QueueChannel hl7MessageChannel() {
        QueueChannel queueChannel = new QueueChannel(20);
        return queueChannel;
    }

    /*
     * Quite a lot going on here!
     */
    @Bean
    IntegrationFlow hl7HandlerIntegrationFlow(MessageChannel hl7MessageChannel,
                                              ThreadPoolTaskScheduler pollerTaskScheduler,
                                              // Spring needs to be explicitly told name when Bean is a List
                                              @Qualifier("hl7HandlerTaskExecutors") List<ThreadPoolTaskExecutor> hl7HandlerTaskExecutors) {
        return IntegrationFlows.from(hl7MessageChannel)
                .bridge(e -> e.poller(Pollers.fixedDelay(10).taskExecutor(pollerTaskScheduler)))
                .enrichHeaders(h -> h.headerFunction(
                        PARTIAL_PARSED_HEADER_KEY,
                        message -> {
                            byte[] asBytes = getBytePayload(message);
                            String asStr = new String(asBytes, StandardCharsets.UTF_8);
                            try {
                                return hl7ParseAndQueue.parseHl7Headers(asStr);
                            } catch (Hl7ParseException e) {
                                throw new RuntimeException(e);
                            }
                        }))
                .route(Message.class,
                        this::getRoutingKey,
                        mapping -> {
                            for (int i = 0; i < HANDLER_PARTITIONS; i++) {
                                String key = getRoutingKeyFromIndex(i);
                                int finalI = i;
                                mapping.subFlowMapping(key, flow ->
                                        flow.channel(
                                                MessageChannels.executor(
                                                        new ErrorHandlingTaskExecutor(
                                                                hl7HandlerTaskExecutors.get(finalI),
                                                                hl7ExecutorErrorHandler)))
                                                .handle(message -> {
                                                    Hl7ParseAndQueue.PartiallyParsedMessage partialParsing =
                                                            getPartialParsing(message);
                                                    hl7ParseAndQueue.saveParseQueue(partialParsing, true);
                                                }));
                            }
                        })
                .get();
    }

    private String getRoutingKey(Message<?> message) {
        Hl7ParseAndQueue.PartiallyParsedMessage partialParsing = getPartialParsing(message);
        String keyStr = partialParsing.bedLocation() + partialParsing.messageTimeslot();
        // hashes can be negative!!
        int key = Math.floorMod(keyStr.hashCode(), HANDLER_PARTITIONS);
        return getRoutingKeyFromIndex(key);
    }

    private static String getRoutingKeyFromIndex(int key) {
        // be 1-indexed like all the thread pools
        return String.format("bucket%02d", key + 1);
    }

    @EventListener
    public void onContextClosed(ContextClosedEvent event) {
        contextShuttingDown.set(true);
    }


    private Hl7ParseAndQueue.PartiallyParsedMessage getPartialParsing(Message<?> message) {
        MessageHeaders headers = message.getHeaders();
        return (Hl7ParseAndQueue.PartiallyParsedMessage) headers.get(PARTIAL_PARSED_HEADER_KEY);
    }

    @SuppressWarnings("unchecked")
    private static byte[] getBytePayload(Message<?> message) {
        return ((Message<byte[]>) message).getPayload();
    }

    /**
     * Executors are created and registered as individual beans here.
     * A List bean for auto-wiring purposes in also created in {@link #hl7HandlerTaskExecutors()},
     * but this is not sufficient to register the executors.
     */
    private void registerHl7HandlerExecutors() {
        for (int i = 0; i < HANDLER_PARTITIONS; i++) {
            ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
            executor.setCorePoolSize(1);
            executor.setMaxPoolSize(1);
            executor.setThreadNamePrefix(String.format("HL7HandlerPart%02d-", i + 1));
            executor.setQueueCapacity(20);
            executor.setWaitForTasksToCompleteOnShutdown(true);
            executor.setAwaitTerminationSeconds(600);
            executor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
            executor.initialize();
            beanFactory.registerSingleton(executorBeanName(i), executor);
        }
    }

    private static String executorBeanName(int i) {
        return String.format("hl7HandlerTaskExecutor%02d", i + 1);
    }

    /**
     * Allow List of executors to be auto-wired. We can't create the executors here
     * because they need to be registered individually so that they'll be shut down correctly.
     * See {@link #registerHl7HandlerExecutors} for where that happens.
     * @return list of all HL7 handler task executor beans
     */
    @Bean
    public List<ThreadPoolTaskExecutor> hl7HandlerTaskExecutors() {
        return IntStream.range(0, HANDLER_PARTITIONS)
                .mapToObj(i -> beanFactory.getBean(executorBeanName(i), ThreadPoolTaskExecutor.class))
                .toList();
    }


}
