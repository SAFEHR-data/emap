package uk.ac.ucl.rits.inform.interchange.springconfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.amqp.RabbitProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.support.RetryTemplate;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.HashMap;
import java.util.Map;

/**
 * Spring configuration for data sources using the AMQP queue.
 *
 * @author Jeremy Stein
 */
@Configuration
public class DataSourceConfiguration {
    private static final Logger logger = LoggerFactory.getLogger(DataSourceConfiguration.class);

    public DataSourceConfiguration(EmapDataSource emapDataSource) {
        this.emapDataSource = emapDataSource;
    }

    /**
     * @return a converter which ensures Instant objects are handled properly
     */
    @Bean
    public MessageConverter jsonMessageConverter() {
        ObjectMapper mapper = new ObjectMapper().findAndRegisterModules();
        return new Jackson2JsonMessageConverter(mapper);
    }

    private final EmapDataSource emapDataSource;

    private @Value("${rabbitmq.queue.length:100000}")
    int queueLength;

    /**
     * @param props RabbitMQ configuration properties
     * @return connectionFactory with publisherConfirms set to true
     */
    @Bean
    @Profile("default")
    public ConnectionFactory connectionFactory(@Autowired RabbitProperties props) {
        CachingConnectionFactory connectionFactory = new CachingConnectionFactory(props.getHost(), props.getPort());
        connectionFactory.setUsername(props.getUsername());
        connectionFactory.setPassword(props.getPassword());
        connectionFactory.setPublisherConfirms(true);
        return connectionFactory;
    }

    /**
     * @param messageConverter the message converter
     * @param connectionFactory the AMQP connection factory
     * @return our rabbit template
     */
    @Bean
    @Profile("default")
    public RabbitTemplate rabbitTemp(@Autowired MessageConverter messageConverter, @Autowired ConnectionFactory connectionFactory) {
        RabbitAdmin rabbitAdmin = new RabbitAdmin(connectionFactory);
        String queueName = getEmapDataSource().getQueueName();
        Map<String, Object> args = new HashMap<>();
        args.put("x-max-length", queueLength);
        args.put("x-overflow", "reject-publish");
        Queue q = new Queue(queueName, true, false, false, args);
        while (true) {
            try {
                rabbitAdmin.declareQueue(q);
                break;
            } catch (AmqpException e) {
                int secondsSleep = 5;
                logger.warn("Creating RabbitMQ queue '{}' failed with exception {}, retrying in {} seconds",
                        queueName, e.getMessage(), secondsSleep);
                try {
                    Thread.sleep(secondsSleep * 1000);
                } catch (InterruptedException e1) {
                    logger.warn("Sleep interrupted");
                }
                continue;
            }
        }

        RetryTemplate retryTemplate = new RetryTemplate();
        ExponentialBackOffPolicy backOffPolicy = new ExponentialBackOffPolicy();
        backOffPolicy.setInitialInterval(500);
        backOffPolicy.setMultiplier(10.0);
        backOffPolicy.setMaxInterval(10000);
        retryTemplate.setBackOffPolicy(backOffPolicy);

        RabbitTemplate template = rabbitAdmin.getRabbitTemplate();
        template.setMessageConverter(messageConverter);
        template.setRetryTemplate(retryTemplate);
        template.setMandatory(true);

        logger.info("Created queue " + queueName + ", properties = " + rabbitAdmin.getQueueProperties(queueName));
        return template;
    }

    /**
     * @return the data source
     */
    public EmapDataSource getEmapDataSource() {
        return emapDataSource;
    }

}
