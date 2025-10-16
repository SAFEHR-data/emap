package uk.ac.ucl.rits.inform.datasinks.emapstar;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import uk.ac.ucl.rits.inform.datasinks.emapstar.controllers.LocationController;
import uk.ac.ucl.rits.inform.datasinks.emapstar.dataprocessors.AdtProcessor;
import uk.ac.ucl.rits.inform.datasinks.emapstar.dataprocessors.AdvanceDecisionProcessor;
import uk.ac.ucl.rits.inform.datasinks.emapstar.dataprocessors.ConsultationRequestProcessor;
import uk.ac.ucl.rits.inform.datasinks.emapstar.dataprocessors.FlowsheetProcessor;
import uk.ac.ucl.rits.inform.datasinks.emapstar.dataprocessors.FormProcessor;
import uk.ac.ucl.rits.inform.datasinks.emapstar.dataprocessors.LabProcessor;
import uk.ac.ucl.rits.inform.datasinks.emapstar.dataprocessors.PatientStateProcessor;
import uk.ac.ucl.rits.inform.datasinks.emapstar.dataprocessors.WaveformProcessor;
import uk.ac.ucl.rits.inform.interchange.AdvanceDecisionMessage;
import uk.ac.ucl.rits.inform.interchange.ConsultMetadata;
import uk.ac.ucl.rits.inform.interchange.ConsultRequest;
import uk.ac.ucl.rits.inform.interchange.EmapOperationMessageProcessingException;
import uk.ac.ucl.rits.inform.interchange.EmapOperationMessageProcessor;
import uk.ac.ucl.rits.inform.interchange.PatientAllergy;
import uk.ac.ucl.rits.inform.interchange.PatientInfection;
import uk.ac.ucl.rits.inform.interchange.PatientProblem;
import uk.ac.ucl.rits.inform.interchange.ResearchOptOut;
import uk.ac.ucl.rits.inform.interchange.adt.AdtMessage;
import uk.ac.ucl.rits.inform.interchange.adt.CancelPendingTransfer;
import uk.ac.ucl.rits.inform.interchange.adt.ChangePatientIdentifiers;
import uk.ac.ucl.rits.inform.interchange.adt.DeletePersonInformation;
import uk.ac.ucl.rits.inform.interchange.adt.MergePatient;
import uk.ac.ucl.rits.inform.interchange.adt.MoveVisitInformation;
import uk.ac.ucl.rits.inform.interchange.adt.PendingTransfer;
import uk.ac.ucl.rits.inform.interchange.adt.SwapLocations;
import uk.ac.ucl.rits.inform.interchange.adt.UpdateSubSpeciality;
import uk.ac.ucl.rits.inform.interchange.form.FormMetadataMsg;
import uk.ac.ucl.rits.inform.interchange.form.FormMsg;
import uk.ac.ucl.rits.inform.interchange.form.FormQuestionMetadataMsg;
import uk.ac.ucl.rits.inform.interchange.lab.LabMetadataMsg;
import uk.ac.ucl.rits.inform.interchange.lab.LabOrderMsg;
import uk.ac.ucl.rits.inform.interchange.location.DepartmentMetadata;
import uk.ac.ucl.rits.inform.interchange.location.LocationMetadata;
import uk.ac.ucl.rits.inform.interchange.visit_observations.Flowsheet;
import uk.ac.ucl.rits.inform.interchange.visit_observations.FlowsheetMetadata;
import uk.ac.ucl.rits.inform.interchange.visit_observations.WaveformMessage;

import javax.annotation.PostConstruct;
import java.time.Instant;

/**
 * All the operations that can be performed on EMAP star db.
 */
@Component
@EntityScan({"uk.ac.ucl.rits.inform.datasinks.emapstar.repos", "uk.ac.ucl.rits.inform.informdb"})
public class InformDbOperations implements EmapOperationMessageProcessor {
    private static final Logger logger = LoggerFactory.getLogger(InformDbOperations.class);

    @Autowired
    private AdtProcessor adtProcessor;
    @Autowired
    private FlowsheetProcessor flowsheetProcessor;
    @Autowired
    private LabProcessor labProcessor;
    @Autowired
    private PatientStateProcessor patientStateProcessor;
    @Autowired
    private ConsultationRequestProcessor consultationRequestProcessor;
    @Autowired
    private LocationController locationController;
    @Autowired
    private AdvanceDecisionProcessor advanceDecisionProcessor;
    @Autowired
    private FormProcessor formProcessor;
    @Autowired
    private WaveformProcessor waveformProcessor;

    @Value("${features.sde:false}")
    private boolean sdeFeatureEnabled;

    @PostConstruct
    public void logFeatureFlags() {
        logger.info("Feature flag: SDE = {}", sdeFeatureEnabled);
    }


    /**
     * Process a lab order message.
     * @param labOrderMsg the message
     * @throws EmapOperationMessageProcessingException if message could not be processed
     */
    @Override
    @Transactional
    public void processMessage(LabOrderMsg labOrderMsg) throws EmapOperationMessageProcessingException {
        Instant storedFrom = Instant.now();
        labProcessor.processMessage(labOrderMsg, storedFrom);
    }

    /**
     * Process a patient allergy message.
     * @param msg the message
     * @throws EmapOperationMessageProcessingException if message could not be processed
     */
    @Override
    @Transactional
    public void processMessage(PatientAllergy msg) throws EmapOperationMessageProcessingException {
        Instant storedFrom = Instant.now();
        patientStateProcessor.processMessage(msg, storedFrom);
    }

    @Override
    public void processMessage(WaveformMessage msg) throws EmapOperationMessageProcessingException {
        Instant storedFrom = Instant.now();
        waveformProcessor.processMessage(msg, storedFrom);
    }

    /**
     * @param msg the ADT message to process
     * @throws EmapOperationMessageProcessingException if message cannot be processed
     */
    @Override
    @Transactional
    public void processMessage(AdtMessage msg) throws EmapOperationMessageProcessingException {
        Instant storedFrom = Instant.now();
        adtProcessor.processMessage(msg, storedFrom);
    }

    /**
     * @param msg the MergeById message to process
     * @throws EmapOperationMessageProcessingException if message cannot be processed
     */
    @Override
    @Transactional
    public void processMessage(MergePatient msg) throws EmapOperationMessageProcessingException {
        Instant storedFrom = Instant.now();
        adtProcessor.processMergePatient(msg, storedFrom);
    }

    /**
     * @param msg the DischargePatient message to process
     * @throws EmapOperationMessageProcessingException if message cannot be processed
     */
    @Override
    @Transactional
    public void processMessage(DeletePersonInformation msg) throws EmapOperationMessageProcessingException {
        Instant storedFrom = Instant.now();
        adtProcessor.deletePersonInformation(msg, storedFrom);
    }

    /**
     * @param msg the MoveVisitInformation message to process
     * @throws EmapOperationMessageProcessingException if message cannot be processed
     */
    @Override
    @Transactional
    public void processMessage(MoveVisitInformation msg) throws EmapOperationMessageProcessingException {
        Instant storedFrom = Instant.now();
        adtProcessor.moveVisitInformation(msg, storedFrom);
    }

    /**
     * @param msg the ChangePatientIdentifiers message to process
     */
    @Override
    @Transactional
    public void processMessage(ChangePatientIdentifiers msg) throws EmapOperationMessageProcessingException {
        Instant storedFrom = Instant.now();
        adtProcessor.changePatientIdentifiers(msg, storedFrom);
    }

    /**
     * @param msg the SwapLocations message to process
     */
    @Override
    @Transactional
    public void processMessage(SwapLocations msg) throws EmapOperationMessageProcessingException {
        Instant storedFrom = Instant.now();
        adtProcessor.swapLocations(msg, storedFrom);
    }

    /**
     * @param msg the PendingTransfer message to process
     */
    @Override
    @Transactional
    public void processMessage(PendingTransfer msg) throws EmapOperationMessageProcessingException {
        Instant storedFrom = Instant.now();
        adtProcessor.processPendingAdt(msg, storedFrom);
    }

    /**
     * @param msg the CancelPendingTransfer message to process
     */
    @Override
    @Transactional
    public void processMessage(CancelPendingTransfer msg) throws EmapOperationMessageProcessingException {
        Instant storedFrom = Instant.now();
        adtProcessor.processPendingAdt(msg, storedFrom);
    }

    /**
     * @param msg the ResearchOptOut msg to process.
     * @throws EmapOperationMessageProcessingException if message cannot be processed
     */
    @Override
    @Transactional
    public void processMessage(ResearchOptOut msg) throws EmapOperationMessageProcessingException {
        Instant storedFrom = Instant.now();
        patientStateProcessor.processMessage(msg, storedFrom);
    }

    @Override
    @Transactional
    public void processMessage(Flowsheet msg) throws EmapOperationMessageProcessingException {
        Instant storedFrom = Instant.now();
        flowsheetProcessor.processMessage(msg, storedFrom);
    }

    /**
     * @param msg the PatientInfection message to process
     * @throws EmapOperationMessageProcessingException if message cannot be processed
     */
    @Override
    @Transactional
    public void processMessage(PatientInfection msg) throws EmapOperationMessageProcessingException {
        Instant storedFrom = Instant.now();
        patientStateProcessor.processMessage(msg, storedFrom);
    }

    /**
     * @param msg the PatientProblem message to process
     * @throws EmapOperationMessageProcessingException if message cannot be processed
     */
    @Override
    @Transactional
    public void processMessage(PatientProblem msg) throws EmapOperationMessageProcessingException {
        Instant storedFrom = Instant.now();
        patientStateProcessor.processMessage(msg, storedFrom);
    }

    /**
     * @param msg the FlowsheetMetadata message to process
     * @throws EmapOperationMessageProcessingException if message cannot be processed
     */
    @Override
    @Transactional
    public void processMessage(FlowsheetMetadata msg) throws EmapOperationMessageProcessingException {
        Instant storedFrom = Instant.now();
        flowsheetProcessor.processMessage(msg, storedFrom);
    }

    @Override
    @Transactional
    public void processMessage(ConsultRequest msg) throws EmapOperationMessageProcessingException {
        Instant storedFrom = Instant.now();
        consultationRequestProcessor.processMessage(msg, storedFrom);
    }

    /**
     * Process Consult Metadata message.
     * @param msg mapping for consult code to human readable data
     */
    @Override
    @Transactional
    public void processMessage(ConsultMetadata msg) {
        Instant storedFrom = Instant.now();
        consultationRequestProcessor.processMessage(msg, storedFrom);
    }

    @Override
    @Transactional
    public void processMessage(LocationMetadata msg) throws EmapOperationMessageProcessingException {
        Instant storedFrom = Instant.now();
        locationController.processMessage(msg, storedFrom);
    }

    @Override
    @Transactional
    public void processMessage(DepartmentMetadata msg) throws EmapOperationMessageProcessingException {
        Instant storedFrom = Instant.now();
        locationController.processMessage(msg, storedFrom);
    }

    @Override
    @Transactional
    public void processMessage(AdvanceDecisionMessage msg) throws EmapOperationMessageProcessingException {
        Instant storedFrom = Instant.now();
        advanceDecisionProcessor.processMessage(msg, storedFrom);
    }

    @Override
    @Transactional
    public void processMessage(LabMetadataMsg msg) throws EmapOperationMessageProcessingException {
        Instant storedFrom = Instant.now();
        labProcessor.processMessage(msg, storedFrom);
    }

    private boolean checkSdeEnabled(Object msg) {
        if (!sdeFeatureEnabled) {
            logger.trace("Ignoring message of type {} due to SDE feature flag being disabled", msg.getClass().getSimpleName());
        }
        return sdeFeatureEnabled;
    }

    @Override
    @Transactional
    public void processMessage(FormMsg msg) throws EmapOperationMessageProcessingException {
        if (!checkSdeEnabled(msg)) {
            return;
        }
        Instant storedFrom = Instant.now();
        formProcessor.processFormMessage(msg, storedFrom);
    }

    /**
     * @param msg the FormMetadataMsg msg to process
     * @throws EmapOperationMessageProcessingException if message cannot be processed
     */
    @Override
    @Transactional
    public void processMessage(FormMetadataMsg msg) throws EmapOperationMessageProcessingException {
        if (!checkSdeEnabled(msg)) {
            return;
        }
        Instant storedFrom = Instant.now();
        formProcessor.processMetadataMessage(msg, storedFrom);
    }

    /**
     * @param msg the FormQuestionMetadataMsg msg to process
     * @throws EmapOperationMessageProcessingException if message cannot be processed
     */
    @Override
    public void processMessage(FormQuestionMetadataMsg msg) {
        if (!checkSdeEnabled(msg)) {
            return;
        }
        Instant storedFrom = Instant.now();
        formProcessor.processQuestionMetadataMessage(msg, storedFrom);
    }

    /**
     * Process an Update Sub Speciality message.
     * @param  updateSubSpeciality the message
     * @throws EmapOperationMessageProcessingException if message could not be processed
     */
    @Override
    @Transactional
    public void processMessage(UpdateSubSpeciality updateSubSpeciality) throws EmapOperationMessageProcessingException {
        Instant storedFrom = Instant.now();
        adtProcessor.processUpdateSubSpeciality(updateSubSpeciality, storedFrom);
    }

}
