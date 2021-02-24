package uk.ac.ucl.rits.inform.datasinks.emapstar.controllers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import uk.ac.ucl.rits.inform.datasinks.emapstar.RowState;
import uk.ac.ucl.rits.inform.datasinks.emapstar.repos.labs.LabIsolateAuditRepository;
import uk.ac.ucl.rits.inform.datasinks.emapstar.repos.labs.LabIsolateRepository;
import uk.ac.ucl.rits.inform.datasinks.emapstar.repos.labs.LabResultAuditRepository;
import uk.ac.ucl.rits.inform.datasinks.emapstar.repos.labs.LabResultRepository;
import uk.ac.ucl.rits.inform.datasinks.emapstar.repos.labs.LabSensitivityAuditRepository;
import uk.ac.ucl.rits.inform.datasinks.emapstar.repos.labs.LabSensitivityRepository;
import uk.ac.ucl.rits.inform.informdb.labs.LabIsolate;
import uk.ac.ucl.rits.inform.informdb.labs.LabIsolateAudit;
import uk.ac.ucl.rits.inform.informdb.labs.LabNumber;
import uk.ac.ucl.rits.inform.informdb.labs.LabResult;
import uk.ac.ucl.rits.inform.informdb.labs.LabResultAudit;
import uk.ac.ucl.rits.inform.informdb.labs.LabSensitivity;
import uk.ac.ucl.rits.inform.informdb.labs.LabSensitivityAudit;
import uk.ac.ucl.rits.inform.informdb.labs.LabTestDefinition;
import uk.ac.ucl.rits.inform.interchange.InterchangeValue;
import uk.ac.ucl.rits.inform.interchange.lab.LabOrderMsg;
import uk.ac.ucl.rits.inform.interchange.lab.LabResultMsg;

import java.time.Instant;

/**
 * Controller for LabResult specific information.
 * @author Stef Piatek
 */
@Component
class LabResultController {
    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final LabResultRepository labResultRepo;
    private final LabResultAuditRepository labResultAuditRepo;
    private final LabIsolateRepository labIsolateRepo;
    private final LabIsolateAuditRepository labIsolateAuditRepo;
    private final LabSensitivityRepository labSensitivityRepo;
    private final LabSensitivityAuditRepository labSensitivityAuditRepo;

    LabResultController(
            LabResultRepository labResultRepo, LabResultAuditRepository labResultAuditRepo,
            LabIsolateRepository labIsolateRepo, LabIsolateAuditRepository labIsolateAuditRepo,
            LabSensitivityRepository labResultSensitivityRepo, LabSensitivityAuditRepository labSensitivityAuditRepo
    ) {
        this.labResultRepo = labResultRepo;
        this.labResultAuditRepo = labResultAuditRepo;
        this.labIsolateRepo = labIsolateRepo;
        this.labIsolateAuditRepo = labIsolateAuditRepo;
        this.labSensitivityRepo = labResultSensitivityRepo;
        this.labSensitivityAuditRepo = labSensitivityAuditRepo;
    }

    @Transactional
    public void processResult(LabTestDefinition testDefinition, LabNumber labNumber, LabResultMsg resultMsg, Instant validFrom, Instant storedFrom) {
        LabResult labResult = updateOrCreateLabResult(labNumber, testDefinition, resultMsg, validFrom, storedFrom);
        // If any lab sensitivities, update or create them
        for (LabOrderMsg sensOrder : resultMsg.getLabSensitivities()) {
            LabIsolate isolate = updateOrCreateIsolate(labResult, resultMsg, validFrom, storedFrom);
            for (LabResultMsg sensResult : sensOrder.getLabResultMsgs()) {
                updateOrCreateSensitivity(isolate, sensResult, validFrom, storedFrom);
            }
        }
    }

    /**
     * Updates or creates lab result.
     * <p>
     * Special processing for microbiology isolates:
     * valueAsText stores the isolate name^text (can also be no growth like NG2^No growth after 2 days)
     * units stores the CFU for an isolate, culturing method for no growth
     * comment stores the clinical notes for the isolate
     * @param labNumber      lab number
     * @param testDefinition test definition
     * @param result         lab result msg
     * @param validFrom      most recent change to results
     * @param storedFrom     time that star encountered the message
     * @return lab result wrapped in row state
     */
    private LabResult updateOrCreateLabResult(
            LabNumber labNumber, LabTestDefinition testDefinition, LabResultMsg result, Instant validFrom, Instant storedFrom) {
        RowState<LabResult, LabResultAudit> resultState;
        if (result.getIsolateCode().isEmpty()) {
            resultState = labResultRepo
                    .findByLabNumberIdAndLabTestDefinitionId(labNumber, testDefinition)
                    .map(r -> new RowState<>(r, result.getResultTime(), storedFrom, false))
                    .orElseGet(() -> createLabResult(labNumber, testDefinition, result.getResultTime(), validFrom, storedFrom));
        } else {
            // multiple isolates in a result, so these don't get overwritten with different isolates
            // get by the isolate code as well as the lab number and test definition
            resultState = labResultRepo
                    .findByLabNumberIdAndLabTestDefinitionIdAndValueAsText(labNumber, testDefinition, result.getIsolateCode())
                    .map(r -> new RowState<>(r, result.getResultTime(), storedFrom, false))
                    .orElseGet(() -> createLabResult(labNumber, testDefinition, result.getResultTime(), validFrom, storedFrom));
        }

        if (!resultState.isEntityCreated() && result.getResultTime().isBefore(resultState.getEntity().getResultLastModifiedTime())) {
            logger.trace("LabResult database is more recent than LabResult message, not updating information");
            return resultState.getEntity();
        }

        updateLabResult(resultState, result);

        resultState.saveEntityOrAuditLogIfRequired(labResultRepo, labResultAuditRepo);
        return resultState.getEntity();
    }

    private RowState<LabResult, LabResultAudit> createLabResult(
            LabNumber labNumber, LabTestDefinition testDefinition, Instant resultModified, Instant validFrom, Instant storedFrom) {
        LabResult labResult = new LabResult(labNumber, testDefinition, resultModified);
        return new RowState<>(labResult, validFrom, storedFrom, true);
    }

    private void updateLabResult(RowState<LabResult, LabResultAudit> resultState, LabResultMsg resultMsg) {
        LabResult labResult = resultState.getEntity();
        resultState.assignInterchangeValue(resultMsg.getUnits(), labResult.getUnits(), labResult::setUnits);
        resultState.assignInterchangeValue(resultMsg.getReferenceLow(), labResult.getRangeLow(), labResult::setRangeLow);
        resultState.assignInterchangeValue(resultMsg.getReferenceHigh(), labResult.getRangeHigh(), labResult::setRangeHigh);
        resultState.assignInterchangeValue(resultMsg.getAbnormalFlag(), labResult.getAbnormalFlag(), labResult::setAbnormalFlag);
        resultState.assignInterchangeValue(resultMsg.getNotes(), labResult.getComment(), labResult::setComment);
        resultState.assignIfDifferent(resultMsg.getResultStatus(), labResult.getResultStatus(), labResult::setResultStatus);

        if (resultMsg.isNumeric()) {
            resultState.assignInterchangeValue(resultMsg.getNumericValue(), labResult.getValueAsReal(), labResult::setValueAsReal);
            resultState.assignIfDifferent(resultMsg.getResultOperator(), labResult.getResultOperator(), labResult::setResultOperator);
        } else if (!resultMsg.getIsolateCode().isEmpty()) {
            // Sadly some custom use of fields for Isolates:
            // result -  have no isolate detected or isolate type
            resultState.assignIfDifferent(resultMsg.getIsolateCode(), labResult.getValueAsText(), labResult::setValueAsText);
            // unit -  CFU (if present)
            resultState.assignInterchangeValue(resultMsg.getStringValue(), labResult.getUnits(), labResult::setUnits);

        } else {
            resultState.assignInterchangeValue(resultMsg.getStringValue(), labResult.getValueAsText(), labResult::setValueAsText);
        }

        if (resultState.isEntityUpdated()) {
            labResult.setResultLastModifiedTime(resultMsg.getResultTime());
        }
    }


    private LabIsolate updateOrCreateIsolate(LabResult labResult, LabResultMsg resultMsg, Instant validFrom, Instant storedFrom) {
        RowState<LabIsolate, LabIsolateAudit> isolateState = labIsolateRepo
                .findByLabResultIdAndIsolateCode(labResult, resultMsg.getIsolateCode())
                .map(isolate -> new RowState<>(isolate, validFrom, storedFrom, false))
                .orElseGet(() -> createLabIsolate(labResult, resultMsg.getIsolateCode(), validFrom, storedFrom));
        LabIsolate labIsolate = isolateState.getEntity();

        isolateState.assignIfDifferent(resultMsg.getIsolateName(), labIsolate.getIsolateName(), labIsolate::setIsolateName);
        isolateState.assignIfDifferent(resultMsg.getCultureType(), labIsolate.getCultureType(), labIsolate::setCultureType);
        isolateState.assignIfDifferent(resultMsg.getIsolateQuantity(), labIsolate.getQuantity(), labIsolate::setQuantity);
        // lab sensitivity clinical notes
        InterchangeValue<String> clinicalInfo = resultMsg.getLabSensitivities().stream()
                .map(LabOrderMsg::getClinicalInformation)
                .findFirst()
                .orElseGet(InterchangeValue::unknown);
        isolateState.assignInterchangeValue(clinicalInfo, labIsolate.getClinicalInformation(), labIsolate::setClinicalInformation);

        isolateState.saveEntityOrAuditLogIfRequired(labIsolateRepo, labIsolateAuditRepo);
        return labIsolate;
    }

    private RowState<LabIsolate, LabIsolateAudit> createLabIsolate(LabResult labResult, String isolateCode, Instant validFrom, Instant storedFrom) {
        LabIsolate isolate = new LabIsolate(labResult, isolateCode);
        return new RowState<>(isolate, validFrom, storedFrom, true);

    }

    private void updateOrCreateSensitivity(LabIsolate isolate, LabResultMsg sensitivityMsg, Instant validFrom, Instant storedFrom) {
        if (sensitivityMsg.getStringValue().isUnknown()) {
            return;
        }
        RowState<LabSensitivity, LabSensitivityAudit> sensitivityState = labSensitivityRepo
                .findByLabIsolateIdAndAgent(isolate, sensitivityMsg.getStringValue().get())
                .map(sens -> new RowState<>(sens, validFrom, storedFrom, false))
                .orElseGet(() -> createSensitivity(isolate, sensitivityMsg.getStringValue().get(), validFrom, storedFrom));

        LabSensitivity sensitivity = sensitivityState.getEntity();
        sensitivityState.assignInterchangeValue(sensitivityMsg.getAbnormalFlag(), sensitivity.getSensitivity(), sensitivity::setSensitivity);
        if (sensitivityState.isEntityUpdated()) {
            sensitivityState.assignIfDifferent(validFrom, sensitivity.getReportingDatetime(), sensitivity::setReportingDatetime);
        }
        sensitivityState.saveEntityOrAuditLogIfRequired(labSensitivityRepo, labSensitivityAuditRepo);
    }

    private RowState<LabSensitivity, LabSensitivityAudit> createSensitivity(
            LabIsolate labIsolate, String agent, Instant validFrom, Instant storedFrom) {
        LabSensitivity sensitivity = new LabSensitivity(labIsolate, agent);
        sensitivity.setReportingDatetime(validFrom);
        return new RowState<>(sensitivity, validFrom, storedFrom, true);
    }

}
