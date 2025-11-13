package uk.ac.ucl.rits.inform.datasources.waveform;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import uk.ac.ucl.rits.inform.interchange.springconfig.EmapRabbitMqRoute;

@Configuration
public class WaveformConfig {
    /**
     * @return the datasource enums denoting which rabbitmq queue/exchange to publish to
     */
    @Bean
    public EmapRabbitMqRoute getRabbitMqRoute() {
        return new EmapRabbitMqRoute(
                // we are publishing to a fanout exchange so the routing key (=queue in a direct exchange)
                // is ignored
                EmapRabbitMqRoute.EmapDataSourceQueue.IGNORED_QUEUE,
                EmapRabbitMqRoute.EmapDataSourceExchange.WAVEFORM_EXCHANGE
        );
    }
}
