package uk.ac.ucl.rits.inform.interchange;

import uk.ac.ucl.rits.inform.interchange.adt.AdtMessage;
import uk.ac.ucl.rits.inform.interchange.adt.CancelPendingTransfer;
import uk.ac.ucl.rits.inform.interchange.adt.ChangePatientIdentifiers;
import uk.ac.ucl.rits.inform.interchange.adt.DeletePersonInformation;
import uk.ac.ucl.rits.inform.interchange.adt.MergePatient;
import uk.ac.ucl.rits.inform.interchange.adt.MoveVisitInformation;
import uk.ac.ucl.rits.inform.interchange.adt.PendingTransfer;
import uk.ac.ucl.rits.inform.interchange.adt.SwapLocations;
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

/**
 * Define the message types that an Emap processor
 * must process.
 * @author Jeremy Stein
 * @author Stef Piatek
 */
public interface EmapOperationMessageProcessor {
    /**
     * @param msg the lab order message to process
     * @throws EmapOperationMessageProcessingException if message cannot be processed
     */
    void processMessage(LabOrderMsg msg) throws EmapOperationMessageProcessingException;

    /**
     * @param msg the ADT message to process
     * @throws EmapOperationMessageProcessingException if message cannot be processed
     */
    void processMessage(AdtMessage msg) throws EmapOperationMessageProcessingException;

    /**
     * @param msg the MergeById message to process
     * @throws EmapOperationMessageProcessingException if message cannot be processed
     */
    void processMessage(MergePatient msg) throws EmapOperationMessageProcessingException;

    /**
     * @param msg the DeletePersonInformation message to process
     * @throws EmapOperationMessageProcessingException if message cannot be processed
     */
    void processMessage(DeletePersonInformation msg) throws EmapOperationMessageProcessingException;

    /**
     * @param msg the MoveVisitInformation message to process
     * @throws EmapOperationMessageProcessingException if message cannot be processed
     */
    void processMessage(MoveVisitInformation msg) throws EmapOperationMessageProcessingException;

    /**
     * @param msg the ChangePatientIdentifiers message to process
     * @throws EmapOperationMessageProcessingException if message cannot be processed
     */
    void processMessage(ChangePatientIdentifiers msg) throws EmapOperationMessageProcessingException;

    /**
     * @param msg the SwapLocations to process
     * @throws EmapOperationMessageProcessingException if the message cannot be processed
     */
    void processMessage(SwapLocations msg) throws EmapOperationMessageProcessingException;

    /**
     * @param msg the PendingTransfer msg to process. May want to make this into generic PendingAdt if we implement admit and discharge.
     * @throws EmapOperationMessageProcessingException if message cannot be processed
     */
    void processMessage(PendingTransfer msg) throws EmapOperationMessageProcessingException;

    /**
     * @param msg the CancelPendingTransfer msg to process. May want to make this into generic CancelPendingAdt if we implement admit and discharge.
     * @throws EmapOperationMessageProcessingException if message cannot be processed
     */
    void processMessage(CancelPendingTransfer msg) throws EmapOperationMessageProcessingException;

    /**
     * @param msg the ResearchOptOut msg to process.
     * @throws EmapOperationMessageProcessingException if message cannot be processed
     */
    void processMessage(ResearchOptOut msg) throws EmapOperationMessageProcessingException;

    /**
     * @param msg the flowsheet message to process
     * @throws EmapOperationMessageProcessingException if message cannot be processed
     */
    void processMessage(Flowsheet msg) throws EmapOperationMessageProcessingException;

    /**
     * @param msg the PatientInfection message to process
     * @throws EmapOperationMessageProcessingException if message cannot be processed
     */
    void processMessage(PatientInfection msg) throws EmapOperationMessageProcessingException;

    /**
     * @param msg the PatientProblem message to process
     * @throws EmapOperationMessageProcessingException if message cannot be processed
     */
    void processMessage(PatientProblem msg) throws EmapOperationMessageProcessingException;

    /**
     * @param msg the FlowsheetMetadata message to process
     * @throws EmapOperationMessageProcessingException if message cannot be processed
     */
    void processMessage(FlowsheetMetadata msg) throws EmapOperationMessageProcessingException;

    /**
     * @param msg the LocationMetadata message to process
     * @throws EmapOperationMessageProcessingException if message cannot be processed
     */
    void processMessage(LocationMetadata msg) throws EmapOperationMessageProcessingException;

    /**
     * @param msg the DepartmentMetadata message to process
     * @throws EmapOperationMessageProcessingException if message cannot be processed
     */
    void processMessage(DepartmentMetadata msg) throws EmapOperationMessageProcessingException;

    /**
     * @param msg the ConsultRequest message to process
     * @throws EmapOperationMessageProcessingException if message cannot be processed
     */
    void processMessage(ConsultRequest msg) throws EmapOperationMessageProcessingException;

    /**
     * Process mapping for consult code -> human readable name.
     * @param msg consult metadata message to process
     * @throws EmapOperationMessageProcessingException if message cannot be processed
     */
    void processMessage(ConsultMetadata msg) throws EmapOperationMessageProcessingException;

    /**
     * @param msg the AdvanceDecisionMessage to process
     * @throws EmapOperationMessageProcessingException if message cannot be processed
     */
    void processMessage(AdvanceDecisionMessage msg) throws EmapOperationMessageProcessingException;

    /**
     * @param msg the LabMetadataMsg to process
     * @throws EmapOperationMessageProcessingException if message cannot be processed
     */
    void processMessage(LabMetadataMsg msg) throws EmapOperationMessageProcessingException;

    /**
     * @param msg the Form msg to process
     * @throws EmapOperationMessageProcessingException if message cannot be processed
     */
    void processMessage(FormMsg msg) throws EmapOperationMessageProcessingException;

    /**
     * @param msg the FormMetadataMsg msg to process
     * @throws EmapOperationMessageProcessingException if message cannot be processed
     */
    void processMessage(FormMetadataMsg msg) throws EmapOperationMessageProcessingException;

    /**
     * @param msg the FormQuestionMetadataMsg msg to process
     * @throws EmapOperationMessageProcessingException if message cannot be processed
     */
    void processMessage(FormQuestionMetadataMsg msg) throws EmapOperationMessageProcessingException;

    /**
     * @param msg the PatientAllergy message to process
     * @throws EmapOperationMessageProcessingException if message cannot be processed
     */
    void processMessage(PatientAllergy msg) throws EmapOperationMessageProcessingException;

    /**
     * @param msg the PatientAllergy message to process
     * @throws EmapOperationMessageProcessingException if message cannot be processed
     */
    void processMessage(WaveformMessage msg) throws EmapOperationMessageProcessingException;
}
