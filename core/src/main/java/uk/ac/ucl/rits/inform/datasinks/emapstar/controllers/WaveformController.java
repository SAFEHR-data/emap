package uk.ac.ucl.rits.inform.datasinks.emapstar.controllers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import uk.ac.ucl.rits.inform.datasinks.emapstar.exceptions.MessageIgnoredException;
import uk.ac.ucl.rits.inform.datasinks.emapstar.repos.LocationVisitRepository;
import uk.ac.ucl.rits.inform.datasinks.emapstar.repos.visit_observations.WaveformRepository;
import uk.ac.ucl.rits.inform.informdb.movement.LocationVisit;
import uk.ac.ucl.rits.inform.informdb.visit_recordings.VisitObservationType;
import uk.ac.ucl.rits.inform.informdb.visit_recordings.Waveform;
import uk.ac.ucl.rits.inform.interchange.InterchangeValue;
import uk.ac.ucl.rits.inform.interchange.visit_observations.WaveformMessage;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

/**
 * Controller for Waveform specific information.
 * @author Jeremy Stein
 */
@Component
public class WaveformController {
    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final WaveformRepository waveformRepository;
    private final LocationVisitRepository locationVisitRepository;

    WaveformController(
            WaveformRepository waveformRepository,
            LocationVisitRepository locationVisitRepository
    ) {
        this.waveformRepository = waveformRepository;
        this.locationVisitRepository = locationVisitRepository;
    }

    /**
     * Process waveform data message.
     * @param msg the interchange message
     * @param visitObservationType to associate with this waveform data
     * @param storedFrom stored from timestamp
     * @throws MessageIgnoredException if message not processed
     */
    @Transactional
    public void processWaveform(
            WaveformMessage msg,
            VisitObservationType visitObservationType,
            Instant storedFrom) throws MessageIgnoredException {
        InterchangeValue<List<Double>> interchangeValue = msg.getNumericValues();
        if (!interchangeValue.isSave()) {
            throw new MessageIgnoredException("Updating/deleting waveform data is not supported");
        }
        // All given values are put into one new row. It's the responsibility of whoever is
        // generating the message to choose an appropriate size of array.
        List<Double> numericValues = interchangeValue.get();
        Instant observationTime = msg.getObservationTime();
        // Try to find the visit. We don't have enough information to create the visit if it doesn't already exist.
        Optional<LocationVisit> inferredLocationVisit =
                locationVisitRepository.findLocationVisitByLocationAndTime(observationTime, msg.getMappedLocationString());
        // XXX: will have to do some sanity checks here to be sure that the HL7 feed hasn't gone down.
        // See issue #36, and here for discussion:
        // https://github.com/UCLH-DHCT/emap/blob/jeremy/hf-data/docs/dev/features/waveform_hf_data.md#core-processor-logic-orphan-data-problem
        Waveform dataRow = new Waveform(
                observationTime,
                observationTime,
                storedFrom);
        inferredLocationVisit.ifPresent(dataRow::setLocationVisitId);
        Double[] valuesAsArray = numericValues.toArray(new Double[0]);
        dataRow.setSamplingRate(msg.getSamplingRate());
        dataRow.setSourceLocation(msg.getSourceLocationString());
        dataRow.setVisitObservationTypeId(visitObservationType);
        dataRow.setUnit(msg.getUnit());
        dataRow.setValuesArray(valuesAsArray);
        waveformRepository.save(dataRow);
    }

    /**
     * Delete waveform data before the cutoff date.
     * @param olderThanCutoff cutoff date
     * @return number of rows deleted
     */
    @Transactional
    public int deleteOldWaveformData(Instant olderThanCutoff) {
        return waveformRepository.deleteAllInBatchByObservationDatetimeBefore(olderThanCutoff);
    }

    /**
     * @return Return observation datetime of most recent waveform data.
     */
    public Instant mostRecentObservationDatatime() {
        return waveformRepository.mostRecentObservationDatatime();
    }
}
