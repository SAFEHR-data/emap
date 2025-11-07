package uk.ac.ucl.rits.inform.datasources.waveform;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import uk.ac.ucl.rits.inform.interchange.springconfig.EmapDataSource;

@Configuration
public class WaveformConfig {
    /**
     * @return the datasource enums denoting which rabbitmq queue/exchange to publish to
     */
    @Bean
    public EmapDataSource getDataSource() {
        return new EmapDataSource(
                EmapDataSource.EmapDataSourceQueue.WAVEFORM_DATA,
                EmapDataSource.EmapDataSourceExchange.WAVEFORM_EXCHANGE
        );
    }

}
