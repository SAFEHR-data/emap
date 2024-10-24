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
import uk.ac.ucl.rits.inform.interchange.visit_observations.WaveformMessage;

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
    @Value("${core.waveform.is_non_current_test_data}")
    private boolean isNonCurrentTestData;

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
    public void processMessage(final WaveformMessage msg, final Instant storedFrom) throws EmapOperationMessageProcessingException {
        VisitObservationType visitObservationType = visitObservationController.getOrCreateFromWaveform(msg, storedFrom);
        waveformController.processWaveform(msg, visitObservationType, storedFrom);
    }


    /**
     * To keep the overall database size down to something reasonable, periodically delete old data.
     */
    @Scheduled(fixedRate = 60 * 60 * 1000)
    public void deleteOldWaveformData() {
        logger.info("deleteOldWaveformData: Checking for old waveform data for deletion");
        Instant baselineDatetime;
        if (isNonCurrentTestData) {
            // while testing, use the current data (which may be for a
            // date far from the present) as a reference for when to apply retention cutoff date from.
            // ie. assume the time of the most recent data is "now"
            baselineDatetime = waveformController.mostRecentObservationDatatime();
            if (baselineDatetime == null) {
                logger.info("deleteOldWaveformData: nothing in DB, do nothing");
                return;
            }

        } else {
            baselineDatetime = Instant.now();
        }
        Instant cutoff = baselineDatetime.minus(retentionTimeHours, ChronoUnit.HOURS);
        logger.info("deleteOldWaveformData: baseline = {}, cutoff = {}", baselineDatetime, cutoff);
        int numDeleted = waveformController.deleteOldWaveformData(cutoff);
        logger.info("deleteOldWaveformData: Old waveform data deletion: {} rows older than {}", numDeleted, cutoff);
    }

}
