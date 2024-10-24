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
import org.springframework.integration.ip.tcp.TcpReceivingChannelAdapter;
import org.springframework.integration.ip.tcp.connection.DefaultTcpNetConnectionSupport;
import org.springframework.integration.ip.tcp.connection.TcpNetConnection;
import org.springframework.integration.ip.tcp.connection.TcpNetServerConnectionFactory;
import org.springframework.integration.ip.tcp.serializer.ByteArraySingleTerminatorSerializer;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import uk.ac.ucl.rits.inform.datasources.waveform.hl7parse.Hl7ParseException;

import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Listen on a TCP port for incoming HL7 messages.
 */
@Configuration
public class Hl7ListenerConfig {
    private final Logger logger = LoggerFactory.getLogger(Hl7ListenerConfig.class);

    private final Hl7ParseAndSend hl7ParseAndSend;

    public Hl7ListenerConfig(Hl7ParseAndSend hl7ParseAndSend) {
        this.hl7ParseAndSend = hl7ParseAndSend;
    }

    /**
     * Specify the server config.
     * @param listenPort port to listen on (inside container)
     * @param sourceAddressAllowList list of source addresses that are allowed to connect to us
     * @param listenTaskExecutor task executor to use for TCP listener
     * @return connection factory
     */
    @Bean
    public TcpNetServerConnectionFactory serverConnectionFactory(
            @Value("${waveform.hl7.listen_port}") int listenPort,
            @Value("${waveform.hl7.source_address_allow_list}") List<String> sourceAddressAllowList,
            ThreadPoolTaskExecutor listenTaskExecutor
    ) {
        TcpNetServerConnectionFactory connFactory = new TcpNetServerConnectionFactory(listenPort);
        connFactory.setSoSendBufferSize(10 * 1024 * 1024);
        connFactory.setSoReceiveBufferSize(10 * 1024 * 1024);
        connFactory.setTaskExecutor(listenTaskExecutor);
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

    @Bean
    ThreadPoolTaskExecutor hl7HandlerTaskExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(4);
        executor.setMaxPoolSize(16);
        executor.setThreadNamePrefix("HL7Handler-");
        executor.setQueueCapacity(5000);
        executor.initialize();
        return executor;
    }

    @Bean
    ThreadPoolTaskExecutor listenTaskExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(4);
        executor.setMaxPoolSize(8);
        executor.setThreadNamePrefix("TcpListen-");
        executor.setQueueCapacity(5000);
        executor.initialize();
        return executor;
    }

    @Bean
    MessageChannel executorStream(ThreadPoolTaskExecutor hl7HandlerTaskExecutor) {
        ExecutorChannel executorChannel = new ExecutorChannel(hl7HandlerTaskExecutor);
        return executorChannel;
    }

    @Bean
    QueueChannel queueTcpStream() {
        QueueChannel queueChannel = new QueueChannel(2000);
        return queueChannel;
    }

    @Bean
    IntegrationFlow integrationFlow(MessageChannel executorStream, MessageChannel queueTcpStream) {
        return IntegrationFlows.from(queueTcpStream)
                .channel(executorStream)
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


    /**
     * Routes the TCP connection to the message handling.
     * @param connectionFactory connection factory
     * @param queueTcpStream message channel for (split) HL7 messages
     * @return adapter
     */
    @Bean
    TcpReceivingChannelAdapter inbound(TcpNetServerConnectionFactory connectionFactory, MessageChannel queueTcpStream) {
        TcpReceivingChannelAdapter adapter = new TcpReceivingChannelAdapter();
        adapter.setConnectionFactory(connectionFactory);
        adapter.setOutputChannel(queueTcpStream);
        return adapter;
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
        // parse message from HL7 to interchange message, send to internal queue
        hl7ParseAndSend.parseAndQueue(asStr);
    }

}
