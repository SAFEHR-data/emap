package uk.ac.ucl.rits.inform.datasources.waveform;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import uk.ac.ucl.rits.inform.interchange.springconfig.EmapDataSource;

@Configuration
public class WaveformConfig {
    /**
     * @return the datasource enum denoting which rabbitmq queue to publish to
     */
    @Bean
    public EmapDataSource getDataSource() {
        return EmapDataSource.WAVEFORM_DATA;
    }

}
