package uk.ac.ucl.rits.inform.datasources.waveform;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.channel.ExecutorChannel;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.dsl.Pollers;
import org.springframework.integration.ip.tcp.TcpReceivingChannelAdapter;
import org.springframework.integration.ip.tcp.connection.DefaultTcpNetConnectionSupport;
import org.springframework.integration.ip.tcp.connection.TcpNetConnection;
import org.springframework.integration.ip.tcp.connection.TcpNetServerConnectionFactory;
import org.springframework.integration.ip.tcp.serializer.ByteArraySingleTerminatorSerializer;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import uk.ac.ucl.rits.inform.datasources.waveform.hl7parse.Hl7ParseException;

import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * Listen on a TCP port for incoming HL7 messages.
 */
@Configuration
public class Hl7ListenerConfig {
    private final Logger logger = LoggerFactory.getLogger(Hl7ListenerConfig.class);

    private final Hl7ParseAndQueue hl7ParseAndQueue;

    public Hl7ListenerConfig(Hl7ParseAndQueue hl7ParseAndQueue) {
        this.hl7ParseAndQueue = hl7ParseAndQueue;
    }

    @Bean
    ThreadPoolTaskExecutor hl7TcpListenTaskExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(4);
        executor.setMaxPoolSize(8);
        executor.setThreadNamePrefix("TcpListen-");
        executor.setQueueCapacity(200);
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
        connFactory.setSoSendBufferSize(10 * 1024 * 1024);
        connFactory.setSoReceiveBufferSize(10 * 1024 * 1024);
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
        QueueChannel queueChannel = new QueueChannel(200);
        return queueChannel;
    }

    @Bean
    IntegrationFlow hl7HandlerIntegrationFlow(MessageChannel hl7MessageChannel,
                                              MessageChannel hl7HandlerChannel,
                                              ThreadPoolTaskScheduler pollerTaskScheduler) {
        return IntegrationFlows.from(hl7MessageChannel)
                .bridge(e -> e.poller(Pollers.fixedDelay(10).taskExecutor(pollerTaskScheduler)))
                .channel(hl7HandlerChannel)
                .handle(msg -> {
                    try {
                        handler((Message<byte[]>) msg);
                    } catch (Hl7ParseException e) {
                        throw new RuntimeException(e);
                    } catch (WaveformCollator.CollationException e) {
                        throw new RuntimeException(e);
                    }
                })
                .get();
    }

    @Bean
    ThreadPoolTaskExecutor hl7HandlerTaskExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(4);
        executor.setMaxPoolSize(4);
        executor.setThreadNamePrefix("HL7Handler-");
        executor.setQueueCapacity(200);
        executor.setWaitForTasksToCompleteOnShutdown(true);
        executor.setAwaitTerminationSeconds(600);
        executor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
        executor.initialize();
        return executor;
    }

    @Bean
    MessageChannel hl7HandlerChannel(ThreadPoolTaskExecutor hl7HandlerTaskExecutor) {
        ExecutorChannel executorChannel = new ExecutorChannel(hl7HandlerTaskExecutor);
        return executorChannel;
    }

    /**
     * Message handler. Source IP check has passed if we get here. No reply is expected.
     * @param msg the incoming message
     * @throws Hl7ParseException if HL7 is invalid or in a form that the ad hoc parser can't handle
     * @throws WaveformCollator.CollationException if the data has a logical error that prevents collation
     */
    public void handler(Message<byte[]> msg) throws Hl7ParseException, WaveformCollator.CollationException {
        byte[] asBytes = msg.getPayload();
        String asStr = new String(asBytes, StandardCharsets.UTF_8);
        // XXX: on second thoughts I think we need to separate out the parsing and queueing,
        // so that we can save here as well, or something...
        // parse message from HL7 to interchange message, send to internal queue
        hl7ParseAndQueue.parseAndQueue(asStr);
    }

}
