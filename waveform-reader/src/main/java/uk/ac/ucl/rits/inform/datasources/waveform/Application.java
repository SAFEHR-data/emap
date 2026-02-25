package uk.ac.ucl.rits.inform.datasources.waveform;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Profile;

/**
 * Spring application entry point.
 * @author Jeremy Stein
 */
@SpringBootApplication(scanBasePackages = {
        "uk.ac.ucl.rits.inform.datasources.waveform",
        "uk.ac.ucl.rits.inform.interchange",
        })
public class Application {
    private final Logger logger = LoggerFactory.getLogger(Application.class);

    /**
     * Keep the application running in normal mode. Without this, it seems to exit immediately.
     * Blocks until shutdown (e.g. SIGTERM), then returns so QueueFlushLifecycle can drain queues.
     * @return CommandLineRunner
     */
    @Bean
    @Profile("!hl7-replay & !test")
    CommandLineRunner keepAliveForLiveMode() {
        return args -> {
            logger.info("Waveform reader running in live mode (TCP listener + scheduled collation)");
            try {
                Thread.currentThread().join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.info("Keep-alive interrupted, shutdown proceeding");
            }
        };
    }

    /**
     * @param args command line args
     */
    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
        // QueueFlushLifecycle stores the exit code in a static variable that survives Bean destruction.
        System.exit(QueueFlushLifecycle.getExitCode().get());
    }


}
