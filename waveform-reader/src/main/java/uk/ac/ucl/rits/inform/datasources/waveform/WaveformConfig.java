package uk.ac.ucl.rits.inform.datasources.waveform;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import uk.ac.ucl.rits.inform.interchange.springconfig.EmapRabbitMqRoute;

import java.util.concurrent.ThreadPoolExecutor;

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

    @Bean
    ThreadPoolTaskExecutor closeFileThreadPoolExecutor() {
        // Executor for file closing. Keep thread count low as
        // we don't want too much simultaneous IO, but perhaps
        // a fair amount of this work is doing the bzip2 compression.
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(2);
        executor.setMaxPoolSize(2);
        // Keep queue on the small side so we don't have a big backlog of open
        // files, especially important for graceful shutdown
        executor.setQueueCapacity(15);
        executor.setThreadNamePrefix("Hl7ArchiveCloser-");
        executor.setWaitForTasksToCompleteOnShutdown(true);
        executor.setAwaitTerminationSeconds(600);
        executor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
        executor.initialize();
        return executor;
    }

}
