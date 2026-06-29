package uk.ac.ucl.rits.inform.interchange.springconfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Spring configuration that provides a default EmapRabbitMqRoute bean.
 * This configuration handles backward compatibility with the deprecated EmapDataSource.
 *
 * If an EmapRabbitMqRoute bean is already defined, this configuration is skipped.
 * Otherwise, it creates an EmapRabbitMqRoute from the deprecated EmapDataSource if available.
 *
 * @author Jeremy Stein
 */
@Configuration
public class EmapRabbitMqRouteConfiguration {
    private static final Logger logger = LoggerFactory.getLogger(EmapRabbitMqRouteConfiguration.class);

    /**
     * If user is using the deprecated {@link EmapDataSource}, then upgrade
     * them to the new way here.
     * They should supply their own {@link EmapRabbitMqRoute} bean in due course.
     *
     * @param deprecatedEmapDataSource the deprecated data source enum
     * @return EmapRabbitMqRoute created from the deprecated EmapDataSource
     * @throws IllegalStateException if neither type of bean is found
     */
    @Bean
    @ConditionalOnMissingBean(EmapRabbitMqRoute.class)
    public EmapRabbitMqRoute emapRabbitMqRoute(
            @Autowired(required = false) EmapDataSource deprecatedEmapDataSource) {
        if (deprecatedEmapDataSource != null) {
            logger.warn("Using deprecated EmapDataSource to create EmapRabbitMqRoute. "
                    + "Please migrate to providing an EmapRabbitMqRoute bean directly.");
            return new EmapRabbitMqRoute(
                    EmapRabbitMqRoute.EmapDataSourceQueue.valueOf(deprecatedEmapDataSource.name()),
                    EmapRabbitMqRoute.EmapDataSourceExchange.DEFAULT_EXCHANGE);
        } else {
            throw new IllegalStateException(
                "No data source bean found. Please provide either an EmapRabbitMqRoute or EmapDataSource bean.");
        }
    }
}

