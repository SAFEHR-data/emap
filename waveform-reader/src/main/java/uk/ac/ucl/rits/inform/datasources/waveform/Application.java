package uk.ac.ucl.rits.inform.datasources.waveform;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

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
     * @param args command line args
     */
    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }


}
