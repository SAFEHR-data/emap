package uk.ac.ucl.rits.inform.datasources.ids.labs;

import ca.uhn.hl7v2.HL7Exception;
import ca.uhn.hl7v2.model.v26.group.ORU_R01_OBSERVATION;
import ca.uhn.hl7v2.model.v26.group.ORU_R01_ORDER_OBSERVATION;
import ca.uhn.hl7v2.model.v26.group.ORU_R01_PATIENT_RESULT;
import ca.uhn.hl7v2.model.v26.message.ORU_R01;
import ca.uhn.hl7v2.model.v26.segment.MSH;
import ca.uhn.hl7v2.model.v26.segment.NTE;
import ca.uhn.hl7v2.model.v26.segment.OBR;
import ca.uhn.hl7v2.model.v26.segment.OBX;
import ca.uhn.hl7v2.model.v26.segment.ORC;
import ca.uhn.hl7v2.model.v26.segment.PID;
import ca.uhn.hl7v2.model.v26.segment.PV1;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.ac.ucl.rits.inform.datasources.ids.exceptions.Hl7InconsistencyException;
import uk.ac.ucl.rits.inform.datasources.ids.exceptions.Hl7MessageIgnoredException;
import uk.ac.ucl.rits.inform.datasources.ids.hl7.parser.PatientInfoHl7;
import uk.ac.ucl.rits.inform.interchange.InterchangeValue;
import uk.ac.ucl.rits.inform.interchange.OrderCodingSystem;
import uk.ac.ucl.rits.inform.interchange.ValueType;
import uk.ac.ucl.rits.inform.interchange.lab.LabOrderMsg;
import uk.ac.ucl.rits.inform.interchange.lab.LabResultMsg;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Build Imaging Lab Orders with results.
 * @author Stef Piatek
 */
public final class ImageLabBuilder extends LabOrderBuilder {
    /**
     * Allowed order control IDs for parsing.
     * <p>
     * ORU R01: RE (results)
     */
    private static final String[] ALLOWED_OC_IDS = {"RE"};
    private static final Logger logger = LoggerFactory.getLogger(ImageLabBuilder.class);
    private static final String QUESTION_SEPARATOR = "=";
    private static final Pattern QUESTION_PATTERN = Pattern.compile(QUESTION_SEPARATOR);
    /**
     * OBX identifiers that will be used to build a report result.
     */
    private static final Set<String> RESULT_OBX_IDENTIFIERS = Set.of("IMP", "GDT", "ADT");
    /**
     * Internal identifier for a report result.
     */
    private static final String REPORT_OBX = ValueType.TEXT.toString();

    @Override
    protected void setLabSpecimenNumber(ORC orc) {
        String labFillerSpecimen = orc.getOrc3_FillerOrderNumber().getEi1_EntityIdentifier().getValueOrEmpty();
        String labPlacerSpecimen = orc.getOrc4_PlacerGroupNumber().getEi1_EntityIdentifier().getValueOrEmpty();
        getMsg().setLabSpecimenNumber(labFillerSpecimen.isEmpty() ? labPlacerSpecimen : labFillerSpecimen);
    }

    private void setOrderInformation(String subMessageSourceId, PatientInfoHl7 patientHl7, OBR obr, ORC orc, Collection<NTE> notes)
            throws HL7Exception, Hl7InconsistencyException {
        setBatteryCodingSystem();
        setSourceAndPatientIdentifiers(subMessageSourceId, patientHl7);
        setQuestions(notes, QUESTION_SEPARATOR, QUESTION_PATTERN);
        populateObrFields(obr, false);
        populateOrderInformation(orc, obr);
        setEpicOrderNumberFromORC();
    }

    private void setEpicOrderNumberFromORC() {
        String orcNumber = getEpicCareOrderNumberOrc();
        if (orcNumber.equals(getMsg().getLabSpecimenNumber())) {
            return;
        }

        getMsg().setEpicCareOrderNumber(InterchangeValue.buildFromHl7(orcNumber));
    }


    /**
     * Construct order details from a Image results (ORU^R01) message.
     * @param subMessageSourceId unique Id from the IDS
     * @param obs                the result group from HAPI (ORU_R01_ORDER_OBSERVATION)
     * @param patientHl7         patient hl7 info
     * @throws HL7Exception              if HAPI does
     * @throws Hl7InconsistencyException if, according to my understanding, the HL7 message contains errors
     */
    private ImageLabBuilder(
            String subMessageSourceId, ORU_R01_ORDER_OBSERVATION obs, PatientInfoHl7 patientHl7) throws HL7Exception, Hl7InconsistencyException {
        super(ALLOWED_OC_IDS, OrderCodingSystem.PACS);
        OBR obr = obs.getOBR();
        List<NTE> notes = obs.getNTEAll();
        setOrderInformation(subMessageSourceId, patientHl7, obr, obs.getORC(), notes);


        Map<String, List<OBX>> obxByIdentifier = getLatestValidResultByIdentifier(obs);

        List<LabResultMsg> results = new ArrayList<>(obs.getOBSERVATIONAll().size());
        for (Map.Entry<String, List<OBX>> entries : obxByIdentifier.entrySet()) {
            ImageLabResultBuilder labResult = new ImageLabResultBuilder(entries.getKey().equals(REPORT_OBX), entries.getValue(), obr);
            try {
                labResult.constructMsg();
                if (!labResult.isIgnored()) {
                    results.add(labResult.getMessage());
                }
            } catch (Hl7InconsistencyException e) {
                logger.error("HL7 inconsistency for message {}", subMessageSourceId, e);
            }
        }
        getMsg().setLabResultMsgs(results);
    }

    /**
     * Group OBX segments by identifier, and return the latest result before an IMP (OBX of opinion) segment.
     * <p>
     * We receive multiple OBX segments for the same identifier, but EPIC only keeps the latest result, unless there's an IMP segment.
     * @param obs observations
     * @return OBX segments grouped by their identifier (using primitive identifier as a fallback)
     * @throws HL7Exception if HAPI does
     */
    private Map<String, List<OBX>> getLatestValidResultByIdentifier(ORU_R01_ORDER_OBSERVATION obs) throws HL7Exception {
        List<OBX> obxSegments = obs.getOBSERVATIONAll().stream().map(ORU_R01_OBSERVATION::getOBX).collect(Collectors.toList());

        Map<String, List<OBX>> obxByIdentifier = new HashMap<>(obs.getOBSERVATIONAll().size());
        String previousIdentifier = null;
        for (OBX obx : obxSegments) {
            String identifier = getIdentifierTypeOrEmpty(obx);
            if (RESULT_OBX_IDENTIFIERS.contains(identifier)) {
                identifier = REPORT_OBX;
            }

            if (!identifier.equals(previousIdentifier)) {
                obxByIdentifier.put(identifier, new ArrayList<>(obs.getOBSERVATIONAll().size()));
            }
            obxByIdentifier.get(identifier).add(obx);
            // update previous tracing identifier data
            previousIdentifier = identifier;
        }
        return obxByIdentifier;
    }

    private String getIdentifierTypeOrEmpty(OBX obx) {
        String identifier = obx.getObx3_ObservationIdentifier().getCwe1_Identifier().getValueOrEmpty();
        if (!identifier.isEmpty()) {
            return identifier;
        }
        try {
            // fallback to primitive identifier
            return obx.getObx3_ObservationIdentifier().getCwe1_Identifier().getExtraComponents().getComponent(0).getData().encode();
        } catch (HL7Exception e) {
            logger.error("Could not parse OBX identifier type", e);
            return "";
        }
    }

    /**
     * Build order with results from ORU R01.
     * @param idsUnid unique Id from the IDS
     * @param oruR01  hl7 message
     * @return interchange messages
     * @throws HL7Exception               if HAPI does
     * @throws Hl7InconsistencyException  if the HL7 message contains errors
     * @throws Hl7MessageIgnoredException if message is ignored
     */
    public static Collection<LabOrderMsg> build(String idsUnid, ORU_R01 oruR01)
            throws HL7Exception, Hl7InconsistencyException, Hl7MessageIgnoredException {
        if (oruR01.getPATIENT_RESULTReps() != 1) {
            throw new Hl7MessageIgnoredException("Not expecting Imaging to have multiple patient results in one message");
        }
        ORU_R01_PATIENT_RESULT patientResults = oruR01.getPATIENT_RESULT();
        List<ORU_R01_ORDER_OBSERVATION> orderObservations = patientResults.getORDER_OBSERVATIONAll();
        MSH msh = (MSH) oruR01.get("MSH");
        PID pid = patientResults.getPATIENT().getPID();
        PV1 pv1 = patientResults.getPATIENT().getVISIT().getPV1();
        PatientInfoHl7 patientInfo = new PatientInfoHl7(msh, pid, pv1);

        List<LabOrderMsg> orders = new ArrayList<>(orderObservations.size());
        int msgSuffix = 0;
        for (ORU_R01_ORDER_OBSERVATION obs : orderObservations) {
            msgSuffix++;
            String subMessageSourceId = String.format("%s_%02d", idsUnid, msgSuffix);
            LabOrderBuilder labOrderBuilder = new ImageLabBuilder(subMessageSourceId, obs, patientInfo);
            labOrderBuilder.addMsgIfAllowedOcId(idsUnid, orders);
        }
        return orders;
    }


}

