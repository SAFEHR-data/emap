package uk.ac.ucl.rits.inform.datasources.waveform_generator;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import uk.ac.ucl.rits.inform.interchange.springconfig.EmapRabbitMqRoute;

@Configuration
public class Config {
    /**
     * Publish synthetic ADT messages to the standard ADT queue.
     * @return config bean
     */
    @Bean
    public EmapRabbitMqRoute getHl7DataSource() {
        return new EmapRabbitMqRoute(
                EmapRabbitMqRoute.EmapDataSourceQueue.HL7_QUEUE,
                EmapRabbitMqRoute.EmapDataSourceExchange.DEFAULT_EXCHANGE
        );
    }
}
