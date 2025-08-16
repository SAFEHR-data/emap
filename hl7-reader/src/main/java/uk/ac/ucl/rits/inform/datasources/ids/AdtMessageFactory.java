package uk.ac.ucl.rits.inform.datasources.ids;

import ca.uhn.hl7v2.HL7Exception;
import ca.uhn.hl7v2.model.DataTypeException;
import ca.uhn.hl7v2.model.Message;
import ca.uhn.hl7v2.model.v26.group.ADT_A39_PATIENT;
import ca.uhn.hl7v2.model.v26.group.ADT_A45_MERGE_INFO;
import ca.uhn.hl7v2.model.v26.message.ADT_A39;
import ca.uhn.hl7v2.model.v26.message.ADT_A45;
import ca.uhn.hl7v2.model.v26.message.ORM_O01;
import ca.uhn.hl7v2.model.v26.message.ORU_R01;
import ca.uhn.hl7v2.model.v26.segment.EVN;
import ca.uhn.hl7v2.model.v26.segment.MRG;
import ca.uhn.hl7v2.model.v26.segment.MSH;
import ca.uhn.hl7v2.model.v26.segment.PID;
import ca.uhn.hl7v2.model.v26.segment.PV1;
import ca.uhn.hl7v2.model.v26.segment.PV2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import uk.ac.ucl.rits.inform.datasources.ids.exceptions.Hl7MessageIgnoredException;
import uk.ac.ucl.rits.inform.datasources.ids.exceptions.Hl7MessageNotImplementedException;
import uk.ac.ucl.rits.inform.datasources.ids.hl7.parser.EthnicGroup;
import uk.ac.ucl.rits.inform.datasources.ids.hl7.parser.PV1Wrap;
import uk.ac.ucl.rits.inform.datasources.ids.hl7.parser.PatientInfoHl7;
import uk.ac.ucl.rits.inform.interchange.InterchangeValue;
import uk.ac.ucl.rits.inform.interchange.adt.AdmitPatient;
import uk.ac.ucl.rits.inform.interchange.adt.AdtCancellation;
import uk.ac.ucl.rits.inform.interchange.adt.AdtMessage;
import uk.ac.ucl.rits.inform.interchange.adt.CancelAdmitPatient;
import uk.ac.ucl.rits.inform.interchange.adt.CancelDischargePatient;
import uk.ac.ucl.rits.inform.interchange.adt.CancelPendingTransfer;
import uk.ac.ucl.rits.inform.interchange.adt.CancelTransferPatient;
import uk.ac.ucl.rits.inform.interchange.adt.ChangePatientIdentifiers;
import uk.ac.ucl.rits.inform.interchange.adt.DeletePersonInformation;
import uk.ac.ucl.rits.inform.interchange.adt.DischargePatient;
import uk.ac.ucl.rits.inform.interchange.adt.ImpliedAdtMessage;
import uk.ac.ucl.rits.inform.interchange.adt.MergePatient;
import uk.ac.ucl.rits.inform.interchange.adt.MoveVisitInformation;
import uk.ac.ucl.rits.inform.interchange.adt.PatientClass;
import uk.ac.ucl.rits.inform.interchange.adt.PendingEvent;
import uk.ac.ucl.rits.inform.interchange.adt.PendingTransfer;
import uk.ac.ucl.rits.inform.interchange.adt.PreviousIdentifiers;
import uk.ac.ucl.rits.inform.interchange.adt.RegisterPatient;
import uk.ac.ucl.rits.inform.interchange.adt.SwapLocations;
import uk.ac.ucl.rits.inform.interchange.adt.TransferPatient;
import uk.ac.ucl.rits.inform.interchange.adt.UpdatePatientInfo;

/**
 * Build an AdtMessage Emap interchange object from an HL7 message.
 * @author Jeremy Stein & Stef Piatek
 */
@Component
public class AdtMessageFactory {
    private static final Logger logger = LoggerFactory.getLogger(AdtMessageFactory.class);
    private final EthnicGroup ethnicGroup;

    /**
     * Default constructor.
     * @param ethnicGroup autowired bean
     */
    public AdtMessageFactory(EthnicGroup ethnicGroup) {
        this.ethnicGroup = ethnicGroup;
    }

    /**
     * Construct from an HL7 message.
     * @param hl7Msg   the HL7 message
     * @param sourceId the unique source Id message
     * @return AdtMessage built from the hl7 message
     * @throws HL7Exception                      if HAPI does
     * @throws Hl7MessageNotImplementedException if the ADT type hasn't been implemented
     * @throws Hl7MessageIgnoredException        if sending application missing from message
     */
    public AdtMessage getAdtMessage(final Message hl7Msg, final String sourceId)
            throws HL7Exception, Hl7MessageNotImplementedException, Hl7MessageIgnoredException {
        MSH msh = getMsh(hl7Msg);
        PID pid = getPid(hl7Msg, false);
        PV1 pv1 = getPv1(hl7Msg, false);
        PV2 pv2 = getPv2(hl7Msg);

        PatientInfoHl7 patientInfoHl7 = new PatientInfoHl7(msh, pid, pv1, pv2);
        EVN evn = getEvn(hl7Msg);
        AdtMessage msg = buildAdtMessageSubclass(patientInfoHl7, hl7Msg, evn, msh.getMessageType().getTriggerEvent().getValueOrEmpty());
        addGenericDataToAdtMessage(sourceId, patientInfoHl7, evn, msg);

        return msg;
    }

    /**
     * Add data that all ADT messages may have.
     * @param sourceId       source Id
     * @param patientInfoHl7 patientInfo
     * @param evn            EVN segment
     * @param msg            AdtMessage to be altered
     * @throws HL7Exception               If HAPI does
     * @throws Hl7MessageIgnoredException if sending application missing from message
     */
    private void addGenericDataToAdtMessage(final String sourceId, final PatientInfoHl7 patientInfoHl7, final EVN evn,
                                            AdtMessage msg) throws HL7Exception, Hl7MessageIgnoredException {
        msg.setSourceMessageId(sourceId);
        String sendingApplication = patientInfoHl7.getSendingApplication();
        if (sendingApplication.isEmpty()) {
            throw new Hl7MessageIgnoredException("No sending application in message");
        }
        msg.setSourceSystem(patientInfoHl7.getSendingApplication());
        // will be replaced if there is an evn segment
        msg.setRecordedDateTime(patientInfoHl7.getMessageTimestamp());
        if (patientInfoHl7.pv1SegmentExists()) {
            // will we want demographics to be included in lab messages too?
            msg.setFullLocationString(InterchangeValue.buildFromHl7(patientInfoHl7.getFullLocationString()));
            msg.setPreviousLocationString(InterchangeValue.buildFromHl7(patientInfoHl7.getPreviousLocation()));
            setPatientClassIfExists(patientInfoHl7, msg);
            msg.setVisitNumber(patientInfoHl7.getVisitNumber());
        }
        if (patientInfoHl7.pv2SegmentExists()) {
            msg.setModeOfArrival(InterchangeValue.buildFromHl7(patientInfoHl7.getModeOfArrivalCode()));
        }
        if (patientInfoHl7.pidSegmentExists()) {
            String ethnicity = ethnicGroup.convertCodeToName(patientInfoHl7.getEthnicGroup());
            msg.setEthnicGroup(InterchangeValue.buildFromHl7(ethnicity));
            msg.setMrn(patientInfoHl7.getMrn());
            msg.setNhsNumber(patientInfoHl7.getNHSNumber());
            msg.setPatientBirthDate(InterchangeValue.buildFromHl7(patientInfoHl7.getPatientBirthDate()));
            msg.setPatientBirthDateTime(InterchangeValue.buildFromHl7(patientInfoHl7.getPatientBirthDateTime()));

            // Despite what the HL7 spec hints at, this death information can occur
            // in any message, not just A03
            String hl7DeathIndicator = patientInfoHl7.getPatientDeathIndicator();
            if ("Y".equals(hl7DeathIndicator)) {
                msg.setPatientIsAlive(new InterchangeValue<>(false));
            } else if ("N".equals(hl7DeathIndicator)) {
                msg.setPatientIsAlive(new InterchangeValue<>(true));
            }
            // set the death time even if indicator says they're not dead (it happens...)
            msg.setPatientDeathDateTime(InterchangeValue.buildFromHl7(patientInfoHl7.getPatientDeathDateTime()));

            msg.setPatientFamilyName(InterchangeValue.buildFromHl7(patientInfoHl7.getPatientFamilyName()));
            msg.setPatientGivenName(InterchangeValue.buildFromHl7(patientInfoHl7.getPatientGivenName()));
            msg.setPatientMiddleName(InterchangeValue.buildFromHl7(patientInfoHl7.getPatientMiddleName()));
            msg.setPatientReligion(InterchangeValue.buildFromHl7(patientInfoHl7.getPatientReligion()));
            msg.setPatientSex(InterchangeValue.buildFromHl7(patientInfoHl7.getPatientSex()));
            msg.setPatientTitle(InterchangeValue.buildFromHl7(patientInfoHl7.getPatientTitle()));
            msg.setPatientZipOrPostalCode(InterchangeValue.buildFromHl7(patientInfoHl7.getPatientZipOrPostalCode()));
        }
        if (evn != null) {
            msg.setRecordedDateTime(HL7Utils.interpretLocalTime(evn.getEvn2_RecordedDateTime()));
            msg.setEventReasonCode(evn.getEvn4_EventReasonCode().getValue());
            msg.setEventOccurredDateTime(HL7Utils.interpretLocalTime(evn.getEvn6_EventOccurred()));
        }
    }

    private void setPatientClassIfExists(PV1Wrap pv1, AdtMessage msg) throws HL7Exception {
        if (pv1.getPatientClass() != null) {
            try {
                msg.setPatientClass(InterchangeValue.buildFromHl7(PatientClass.findByHl7Code(pv1.getPatientClass())));
            } catch (IllegalArgumentException e) {
                throw new HL7Exception("Patient class was not recognised", e);
            }
        }
    }

    /**
     * For building up test IDS, get patient information from HL7 message.
     * @param hl7Msg HL7 message
     * @return PatientInfoHl7 object
     * @throws HL7Exception if HAPI does
     */
    public PatientInfoHl7 getPatientInfo(final Message hl7Msg) throws HL7Exception {
        MSH msh = getMsh(hl7Msg);
        PID pid = getPid(hl7Msg, false);
        PV1 pv1 = getPv1(hl7Msg, false);
        return new PatientInfoHl7(msh, pid, pv1);
    }

    /**
     * Build correct AdtMessage subtype and add any specific data.
     * @param pv1Wrap      PV1 wrap for patient
     * @param hl7Msg       HL7 message
     * @param evn          EVN segment, required for any cancellation event
     * @param triggerEvent ADT message trigger event
     * @return ADT Message build with specific subtype added
     * @throws HL7Exception                      if hl7 message can't be parsed
     * @throws Hl7MessageNotImplementedException if message hasn't been implemented yet
     */
    private AdtMessage buildAdtMessageSubclass(final PV1Wrap pv1Wrap, final Message hl7Msg,
                                               final EVN evn, final String triggerEvent) throws HL7Exception, Hl7MessageNotImplementedException {

        AdtMessage msg;
        switch (triggerEvent) {
            case "A01":
                AdmitPatient admitPatient = new AdmitPatient();
                admitPatient.setAdmissionDateTime(InterchangeValue.buildFromHl7(pv1Wrap.getAdmissionDateTime()));
                admitPatient.setAdmissionType(InterchangeValue.buildFromHl7(pv1Wrap.getAdmissionType()));
                msg = admitPatient;
                break;
            case "A02":
            case "A06":
            case "A07":
                TransferPatient transferPatient = new TransferPatient();
                transferPatient.setAdmissionDateTime(InterchangeValue.buildFromHl7(pv1Wrap.getAdmissionDateTime()));
                msg = transferPatient;
                break;
            case "A03":
                DischargePatient dischargeMsg = new DischargePatient();
                dischargeMsg.setAdmissionDateTime(InterchangeValue.buildFromHl7(pv1Wrap.getAdmissionDateTime()));
                dischargeMsg.setDischargeDateTime(pv1Wrap.getDischargeDateTime());
                dischargeMsg.setDischargeDisposition(pv1Wrap.getDischargeDisposition());
                dischargeMsg.setDischargeLocation(pv1Wrap.getDischargeLocation());
                msg = dischargeMsg;
                break;
            case "A04":
                if ("ENC_CREATE".equals(evn.getEvn4_EventReasonCode().getValueOrEmpty())) {
                    throw new Hl7MessageNotImplementedException("ENC_CREATE not implemented");
                }
                RegisterPatient registerPatient = new RegisterPatient();
                registerPatient.setPresentationDateTime(InterchangeValue.buildFromHl7(pv1Wrap.getAdmissionDateTime()));
                msg = registerPatient;
                break;
            case "A08":
            case "A28":
            case "A31":
                msg = new UpdatePatientInfo();
                break;
            // We are receiving A05 and A14, A38 messages but are not implementing scheduling
            case "A14":
            case "A38":
                throw new Hl7MessageNotImplementedException(String.format("Scheduling ADT trigger event not implemented: %s", triggerEvent));
            case "R01": // build implied adt from non-ADT HL7 messages
            case "O01":
            case "A05":
                // to implement patient infections, only parsing as a update patient info from message
                // will need to parse the scheduling information when implemented
                msg = new ImpliedAdtMessage();
                break;
            case "A11":
                CancelAdmitPatient cancelAdmitPatient = new CancelAdmitPatient();
                setCancellationDatetime(evn, cancelAdmitPatient);
                msg = cancelAdmitPatient;
                break;
            case "A12":
                CancelTransferPatient cancelTransferPatient = new CancelTransferPatient();
                setCancellationDatetime(evn, cancelTransferPatient);
                cancelTransferPatient.setCancelledLocation(pv1Wrap.getPreviousLocation());
                msg = cancelTransferPatient;
                break;
            case "A13":
                CancelDischargePatient cancelDischargePatient = new CancelDischargePatient();
                setCancellationDatetime(evn, cancelDischargePatient);
                msg = cancelDischargePatient;
                break;
            case "A15":
                PendingTransfer pendingTransfer = new PendingTransfer();
                setPendingDestination(pv1Wrap, pendingTransfer);
                setHospitalService(pv1Wrap, pendingTransfer);
                msg = pendingTransfer;
                break;
            case "A17":
                msg = buildSwapLocations(hl7Msg, pv1Wrap);
                break;
            case "A26":
                CancelPendingTransfer cancelPendingTransfer = new CancelPendingTransfer();
                setPendingDestination(pv1Wrap, cancelPendingTransfer);
                setCancellationDatetime(evn, cancelPendingTransfer);
                msg = cancelPendingTransfer;
                break;
            case "A29":
                msg = new DeletePersonInformation();
                break;
            case "A40":
                MergePatient mergeMsg = new MergePatient();
                setPreviousIdentifiers(mergeMsg, hl7Msg);
                msg = mergeMsg;
                break;
            case "A45":
                MoveVisitInformation moveVisitInformation = new MoveVisitInformation();
                MRG mrg = setPreviousIdentifiers(moveVisitInformation, hl7Msg);
                moveVisitInformation.setPreviousVisitNumber(mrg.getMrg5_PriorVisitNumber().getIDNumber().toString());
                msg = moveVisitInformation;
                break;
            case "A47":
                ChangePatientIdentifiers changePatientIdentifiers = new ChangePatientIdentifiers();
                setPreviousIdentifiers(changePatientIdentifiers, hl7Msg);
                msg = changePatientIdentifiers;
                break;
            default:
                throw new Hl7MessageNotImplementedException(String.format("Unimplemented ADT trigger event %s", triggerEvent));
        }
        return msg;
    }

    private MRG setPreviousIdentifiers(PreviousIdentifiers msg, Message hl7Msg) throws HL7Exception {
        MRG mrg = getMrg(hl7Msg);

        assert (null != mrg);
        msg.setPreviousMrn(mrg.getMrg1_PriorPatientIdentifierList(0).getIDNumber().toString());
        msg.setPreviousNhsNumber(mrg.getMrg1_PriorPatientIdentifierList(1).getIDNumber().toString());
        return mrg;
    }

    private SwapLocations buildSwapLocations(Message hl7Msg, PV1Wrap pv1Wrap) throws HL7Exception {
        PID pid = getPid(hl7Msg, true);
        PV1 pv1 = getPv1(hl7Msg, true);

        PatientInfoHl7 otherPatientInfo = new PatientInfoHl7(null, pid, pv1, null);
        SwapLocations msg = new SwapLocations();
        msg.setOtherVisitNumber(otherPatientInfo.getVisitNumber());
        msg.setOtherFullLocationString(InterchangeValue.buildFromHl7(otherPatientInfo.getFullLocationString()));

        msg.setOtherMrn(otherPatientInfo.getMrn());
        msg.setOtherNhsNumber(otherPatientInfo.getNHSNumber());

        return msg;
    }

    /**
     * get MRG segment from hl7 message.
     * @param hl7Msg hl7 message
     * @return MRG segment
     * @throws HL7Exception if HAPI does
     */
    private MRG getMrg(Message hl7Msg) throws HL7Exception {
        MRG mrg;
        if (hl7Msg instanceof ADT_A39) {
            ADT_A39_PATIENT a39Patient = (ADT_A39_PATIENT) hl7Msg.get("PATIENT");
            mrg = a39Patient.getMRG();
        } else if (hl7Msg instanceof ADT_A45) {
            ADT_A45_MERGE_INFO mergeInfo = (ADT_A45_MERGE_INFO) hl7Msg.get("MERGE_INFO");
            mrg = mergeInfo.getMRG();
        } else {
            mrg = (MRG) hl7Msg.get("MRG");
        }
        return mrg;
    }


    /**
     * get EVN segment if it exists.
     * @param hl7Msg hl7 message
     * @return EVN segment
     */
    private EVN getEvn(Message hl7Msg) {
        EVN evn = null;
        try {
            evn = (EVN) hl7Msg.get("EVN");
        } catch (HL7Exception e) {
            // EVN is allowed not to exist
        }
        return evn;
    }

    /**
     * get PV1 segment if it exists.
     * Parses from ADT sources, and non-ADT sources (e.g. Lab)
     * @param hl7Msg        hl7 message
     * @param secondSegment get the second segment
     * @return PV1
     * @throws HL7Exception if required segment doesn't exist
     */
    private PV1 getPv1(Message hl7Msg, boolean secondSegment) throws HL7Exception {
        PV1 pv1 = null;
        String segmentName = secondSegment ? "PV12" : "PV1";

        if (!(hl7Msg instanceof ADT_A45)) {
            try {
                pv1 = (PV1) hl7Msg.get(segmentName);
            } catch (HL7Exception e) {
                // some sections are allowed not to exist
            }
        } else {
            ADT_A45_MERGE_INFO mergeInfo = (ADT_A45_MERGE_INFO) hl7Msg.get("MERGE_INFO");
            pv1 = mergeInfo.getPV1();
        }
        // non ADT message processing
        if (hl7Msg instanceof ORU_R01) {
            ORU_R01 oruR01 = (ORU_R01) hl7Msg;
            pv1 = oruR01.getPATIENT_RESULT().getPATIENT().getVISIT().getPV1();
        } else if (hl7Msg instanceof ORM_O01) {
            ORM_O01 ormO01 = (ORM_O01) hl7Msg;
            pv1 = ormO01.getPATIENT().getPATIENT_VISIT().getPV1();
        }

        return pv1;
    }

    private PV2 getPv2(Message hl7Msg) {
        PV2 pv2 = null;
        try {
            pv2 = (PV2) hl7Msg.get("PV2");
        } catch (HL7Exception e) {
            // allowed to not exist
        }
        return pv2;
    }

    /**
     * Get PID if it exists.
     * Parses from ADT sources, and non-ADT sources (e.g. Lab)
     * @param hl7Msg        hl7 message
     * @param secondSegment Get the second segment
     * @return PID
     * @throws HL7Exception if PID doesn't exist for a merge message
     */
    private PID getPid(Message hl7Msg, boolean secondSegment) throws HL7Exception {
        PID pid = null;
        if (hl7Msg instanceof ADT_A39) {
            ADT_A39_PATIENT a39Patient = (ADT_A39_PATIENT) hl7Msg.get("PATIENT");
            pid = a39Patient.getPID();
        } else {
            String segmentName = secondSegment ? "PID2" : "PID";

            try {
                pid = (PID) hl7Msg.get(segmentName);
            } catch (HL7Exception e) {
                // empty PID2 is allowed
            }
        }
        // processing of non ADT messages
        if ((hl7Msg instanceof ORU_R01)) {
            ORU_R01 msg = (ORU_R01) hl7Msg;
            pid = msg.getPATIENT_RESULT().getPATIENT().getPID();
        } else if (hl7Msg instanceof ORM_O01) {
            ORM_O01 ormO01 = (ORM_O01) hl7Msg;
            pid = ormO01.getPATIENT().getPID();
        }
        return pid;
    }

    /**
     * get MSH segment.
     * @param hl7Msg hl7 message
     * @return MSH segment
     * @throws HL7Exception if MSH doesn't exist
     */
    private MSH getMsh(Message hl7Msg) throws HL7Exception {
        return (MSH) hl7Msg.get("MSH");
    }

    /**
     * Set cancellation time from the evn segment.
     * @param evn             EVN segment
     * @param adtCancellation adt cancellation message
     * @throws DataTypeException if the datetime can't be interpreted as local time
     */
    private void setCancellationDatetime(final EVN evn, AdtCancellation adtCancellation) throws DataTypeException {
        adtCancellation.setCancelledDateTime(HL7Utils.interpretLocalTime(evn.getEvn6_EventOccurred()));
    }

    private void setPendingDestination(PV1Wrap pv1Wrap, PendingEvent pendingEvent) {
        String pendingLocation = pv1Wrap.getPendingLocation();
        pendingEvent.setPendingDestination(InterchangeValue.buildFromHl7(pendingLocation));
    }

    private void setHospitalService(PV1Wrap pv1Wrap, PendingEvent pendingEvent) throws HL7Exception {
        String hospitalService = pv1Wrap.getHospitalService();
        pendingEvent.setHospitalService(InterchangeValue.buildFromHl7(hospitalService));
    }
}
