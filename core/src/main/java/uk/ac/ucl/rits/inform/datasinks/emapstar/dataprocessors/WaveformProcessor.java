package uk.ac.ucl.rits.inform.datasinks.emapstar.dataprocessors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import uk.ac.ucl.rits.inform.datasinks.emapstar.controllers.VisitObservationController;
import uk.ac.ucl.rits.inform.datasinks.emapstar.controllers.WaveformController;
import uk.ac.ucl.rits.inform.informdb.visit_recordings.VisitObservationType;
import uk.ac.ucl.rits.inform.interchange.EmapOperationMessageProcessingException;
import uk.ac.ucl.rits.inform.interchange.visit_observations.WaveformHighFreqMessage;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

/**
 * Handle processing of Waveform messages.
 * @author Jeremy Stein
 */
@Component
public class WaveformProcessor {
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final VisitObservationController visitObservationController;
    private final WaveformController waveformController;

    @Value("${core.waveform.retention_hours}")
    private int retentionTimeHours;

    /**
     * @param visitObservationController visit observation controller
     * @param waveformController         waveform controller
     */
    public WaveformProcessor(
            VisitObservationController visitObservationController,
            WaveformController waveformController) {
        this.visitObservationController = visitObservationController;
        this.waveformController = waveformController;
    }

    /**
     * Process waveform message.
     * @param msg        message
     * @param storedFrom Time the message started to be processed by star
     * @throws EmapOperationMessageProcessingException if message can't be processed.
     */
    @Transactional
    public void processMessage(final WaveformHighFreqMessage msg, final Instant storedFrom) throws EmapOperationMessageProcessingException {
        VisitObservationType visitObservationType = visitObservationController.getOrCreateFromWaveform(msg, storedFrom);
        waveformController.processWaveform(msg, visitObservationType, storedFrom);
    }


    /**
     * To keep the overall database size down to something reasonable, periodically delete old data.
     */
    @Scheduled(fixedRate = 60 * 60 * 1000)
    public void deleteOldWaveformData() {
        /* When calculating the retention cutoff datetime, instead of working back from the current datetime,
         * start at the datetime of the most recent piece of waveform data.
         * The main purpose of this is that when testing (eg. using a dump file that might be quite old),
         * you don't want to immediately delete all the data due to its timestamps being way in the past.
         * And in production the most recent piece of data will be very close to the present time anyway,
         * so keep things simple and use the same logic in both cases.
         */
        Instant baselineDatetime = waveformController.mostRecentObservationDatatime();
        if (baselineDatetime == null) {
            logger.info("deleteOldWaveformData: nothing in DB, do nothing");
            return;
        }

        Instant now = Instant.now();
        if (baselineDatetime.isAfter(now)) {
            // In the hopefully unlikely case that the incoming data is in the future, don't
            // go and delete all our data!
            logger.warn("deleteOldWaveformData: most recent data is in the future ({}), using current time instead",
                    baselineDatetime);
            baselineDatetime = now;
        }

        if (retentionTimeHours <= 0) {
            logger.info("deleteOldWaveformData: retention time is infinite, do nothing (baseline date = {})",
                    baselineDatetime);
            return;
        }
        Instant cutoff = baselineDatetime.minus(retentionTimeHours, ChronoUnit.HOURS);
        logger.info("deleteOldWaveformData: deleting, baseline date = {}, cutoff = {}", baselineDatetime, cutoff);
        int numDeleted = waveformController.deleteOldWaveformData(cutoff);
        logger.info("deleteOldWaveformData: deleted {} rows older than {}", numDeleted, cutoff);
    }

}
