package uk.ac.ucl.rits.inform.datasources.waveform;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

/**
 * Scheduling only to be enabled when not running unit tests.
 */
@Configuration
@EnableScheduling
@Profile("!test")
public class SchedulingConfig {
    private final Logger logger = LoggerFactory.getLogger(SchedulingConfig.class);

    @Value("${test.reader.scheduler_pool_size:2}")
    private int schedulerTaskPoolSize;

    /**
     * By default, Spring Integration shares the same task scheduler with any methods you have marked
     * with @Scheduled. And it seems to tie up its thread for a long time, resulting in your scheduled
     * tasks being starved of places to run.
     * I have wasted enough time trying to get them to use two
     * separate schedulers, so let's just use one sufficiently large scheduler. Ie. *at least* two threads.
     * @return custom task scheduler
     */
    @Bean
    ThreadPoolTaskScheduler taskScheduler() {
        ThreadPoolTaskScheduler threadPoolTaskScheduler = new ThreadPoolTaskScheduler();
        threadPoolTaskScheduler.setPoolSize(schedulerTaskPoolSize);
        threadPoolTaskScheduler.setErrorHandler(e -> {
            logger.error("Exception in waveform-reader scheduled task. ", e);
        });
        threadPoolTaskScheduler.setThreadNamePrefix("Scheduler");
        threadPoolTaskScheduler.initialize();
        return threadPoolTaskScheduler;
    }
}
