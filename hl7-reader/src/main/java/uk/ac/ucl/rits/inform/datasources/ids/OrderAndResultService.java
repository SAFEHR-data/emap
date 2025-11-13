package uk.ac.ucl.rits.inform.datasources.ids;

import ca.uhn.hl7v2.HL7Exception;
import ca.uhn.hl7v2.model.v26.message.ORM_O01;
import ca.uhn.hl7v2.model.v26.message.ORR_O02;
import ca.uhn.hl7v2.model.v26.message.ORU_R01;
import ca.uhn.hl7v2.model.v26.message.ORU_R30;
import ca.uhn.hl7v2.model.v26.segment.MSH;
import ca.uhn.hl7v2.model.v26.segment.OBR;
import org.springframework.stereotype.Component;
import uk.ac.ucl.rits.inform.datasources.ids.exceptions.Hl7InconsistencyException;
import uk.ac.ucl.rits.inform.datasources.ids.exceptions.Hl7MessageIgnoredException;
import uk.ac.ucl.rits.inform.datasources.ids.labs.LabFunnel;
import uk.ac.ucl.rits.inform.interchange.EmapOperationMessage;
import uk.ac.ucl.rits.inform.interchange.OrderCodingSystem;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;

/**
 * Decides what type of order or result and sends messages to the correct class and method.
 * <p>
 * Determines the correct class to build messages from (Flowsheets, Labs, Bloods...)
 * If required, determines the coding system to be used by the building class.
 * @author Stef Piatek
 */
@Component
public class OrderAndResultService {
    private final FlowsheetFactory flowsheetFactory;
    private final ConsultFactory consultFactory;
    private final AdvanceDecisionFactory advanceDecisionFactory;
    private static final Set<String> IMG_RESULT_APPS = Set.of("IMG_RESULT", "ELR_RESULT", "IMG_ADDENDUM", "IMG_PROC_CHANGE_RESULT");

    /**
     * Minimal constructor for order and result service.
     * @param flowsheetFactory       Flowsheet factory for respective messages.
     * @param consultFactory         Consult request factory for respective messages.
     * @param advanceDecisionFactory Advanced decision factory for handling respective messages.
     */
    public OrderAndResultService(FlowsheetFactory flowsheetFactory, ConsultFactory consultFactory,
                                 AdvanceDecisionFactory advanceDecisionFactory) {
        this.flowsheetFactory = flowsheetFactory;
        this.consultFactory = consultFactory;
        this.advanceDecisionFactory = advanceDecisionFactory;
    }

    /**
     * Build messages from hl7 message.
     * <p>
     * blood product ORM O01 -> Blood product
     * Consult Orders ORM O01 -> Consult Order
     * all other ORM O01 -> LabOrder
     * @param sourceId unique Id from the IDS
     * @param msg      hl7 message
     * @return Orders or blood product interchange messages
     * @throws HL7Exception               if HAPI does
     * @throws Hl7InconsistencyException  if something about the HL7 message doesn't make sense
     * @throws Hl7MessageIgnoredException if coding system not implemented
     */
    Collection<? extends EmapOperationMessage> buildMessages(String sourceId, ORM_O01 msg)
            throws Hl7InconsistencyException, HL7Exception, Hl7MessageIgnoredException {
        MSH msh = msg.getMSH();
        String sendingApplication = msh.getMsh3_SendingApplication().getHd1_NamespaceID().getValueOrEmpty();
        String sendingFacility = msh.getMsh4_SendingFacility().getHd1_NamespaceID().getValueOrEmpty();
        OBR obr = msg.getORDER().getORDER_DETAIL().getOBR();

        OrderCodingSystem codingSystem = determineCodingSystem(obr, sendingApplication, sendingFacility);
        switch (codingSystem) {
            case BLOOD_PRODUCTS:
                throw new Hl7MessageIgnoredException("Bank Manager products not implemented for now");
            case CONSULT_ORDER:
                return Collections.singleton(consultFactory.makeConsult(sourceId, msg));
            case ADVANCED_DECISION_ORDER:
                return Collections.singleton(advanceDecisionFactory.makeAdvancedDecision(sourceId, msg));
            default:
                // Lab Funnel will throw message ignored exception if not a parsed type (e.g. flowsheet)
                return LabFunnel.buildMessages(sourceId, msg, codingSystem);
        }
    }

    /**
     * Build messages from hl7 message.
     * <p>
     * All ORR O02 -> LabOrder for WinPath or CoPath
     * @param sourceId unique Id from the IDS
     * @param msg      hl7 message
     * @return LabOrder interchange messages
     * @throws HL7Exception               if HAPI does
     * @throws Hl7InconsistencyException  if something about the HL7 message doesn't make sense
     * @throws Hl7MessageIgnoredException if coding system not implemented
     */
    Collection<? extends EmapOperationMessage> buildMessages(String sourceId, ORR_O02 msg)
            throws Hl7MessageIgnoredException, Hl7InconsistencyException, HL7Exception {
        OBR obr = msg.getRESPONSE().getORDER().getOBR();
        OrderCodingSystem codingSystem = determineCodingSystem(obr);
        // Lab Funnel will throw message ignored exception if not a parsed type
        return LabFunnel.buildMessages(sourceId, msg, codingSystem);
    }

    /**
     * Build messages from hl7 message.
     * <p>
     * Vitals ORU R01 -> Flowsheets
     * blood product ORU R01 -> BloodProducts
     * all other ORU R01 -> LabOrder with Results
     * @param sourceId unique Id from the IDS
     * @param msg      hl7 message
     * @return LabOrder interchange messages
     * @throws HL7Exception               if HAPI does
     * @throws Hl7InconsistencyException  if something about the HL7 message doesn't make sense
     * @throws Hl7MessageIgnoredException if coding system not implemented
     */
    Collection<? extends EmapOperationMessage> buildMessages(String sourceId, ORU_R01 msg)
            throws Hl7MessageIgnoredException, Hl7InconsistencyException, HL7Exception {
        MSH msh = msg.getMSH();
        String sendingApplication = msh.getMsh3_SendingApplication().getHd1_NamespaceID().getValueOrEmpty();
        String sendingFacility = msh.getMsh4_SendingFacility().getHd1_NamespaceID().getValueOrEmpty();

        OBR obr = msg.getPATIENT_RESULT().getORDER_OBSERVATION().getOBR();
        OrderCodingSystem codingSystem = determineCodingSystem(obr, sendingApplication, sendingFacility);
        switch (codingSystem) {
            case BLOOD_PRODUCTS:
                throw new Hl7MessageIgnoredException("Bank Manager blood products not implemented for now");
            case FLOWSHEET:
                return flowsheetFactory.getMessages(sourceId, msg);
            default:
                // Lab Funnel will throw message ignored exception if not a parsed type
                return LabFunnel.buildMessages(sourceId, msg, codingSystem);
        }
    }

    /**
     * Build messages from hl7 message.
     * <p>
     * ORU R30 -> LabOrder with results from ABL 90 Flex
     * @param sourceId unique Id from the IDS
     * @param msg      hl7 message
     * @return LabOrder interchange messages
     * @throws HL7Exception               if HAPI does
     * @throws Hl7InconsistencyException  if something about the HL7 message doesn't make sense
     * @throws Hl7MessageIgnoredException if coding system not implemented
     */
    Collection<? extends EmapOperationMessage> buildMessages(String sourceId, ORU_R30 msg)
            throws Hl7MessageIgnoredException, Hl7InconsistencyException, HL7Exception {
        MSH msh = msg.getMSH();
        String sendingApplication = msh.getMsh3_SendingApplication().getHd1_NamespaceID().getValueOrEmpty();
        OrderCodingSystem codingSystem = determineCodingSystem(msg.getOBR(), sendingApplication);
        return LabFunnel.buildMessages(sourceId, msg, codingSystem);
    }

    private OrderCodingSystem determineCodingSystem(OBR obr) throws Hl7MessageIgnoredException {
        return determineCodingSystem(obr, "", "");
    }

    private OrderCodingSystem determineCodingSystem(OBR obr, String sendingApplication) throws Hl7MessageIgnoredException {
        return determineCodingSystem(obr, sendingApplication, "");
    }

    /**
     * Determine the coding system for a result type.
     * <p>
     * Should determine all orders and results that can't always be determined by sender facility (flowsheet) or message type (ABL 90 flex).
     * WinPath and CoPath sometimes have information in MSH, but not always - need to determine from OBR.
     * @param obr                OBR segment
     * @param sendingApplication sender application
     * @param sendingFacility    sending facility
     * @return order coding system
     * @throws Hl7MessageIgnoredException if coding system cannot be parsed
     */
    private OrderCodingSystem determineCodingSystem(OBR obr, String sendingApplication, String sendingFacility) throws Hl7MessageIgnoredException {
        String fillerNamespace = obr.getObr3_FillerOrderNumber().getEi2_NamespaceID().getValueOrEmpty();
        String codingSystem = obr.getObr4_UniversalServiceIdentifier().getCwe3_NameOfCodingSystem().getValueOrEmpty();
        String alternativeIdentifier = obr.getObr4_UniversalServiceIdentifier().getCwe4_AlternateIdentifier().getValueOrEmpty();

        if ("WinPath".equals(codingSystem) || (codingSystem.isBlank() && "WinPath".equals(sendingApplication))) {
            return OrderCodingSystem.WIN_PATH;
        } else if ("CoPathPlus".equals(fillerNamespace) || "CPEAP".equals(codingSystem)) {
            return OrderCodingSystem.CO_PATH;
        }

        switch (sendingApplication) {
            case "BIO-CONNECT":
                return OrderCodingSystem.BIO_CONNECT;
            case "ABL90 FLEX Plus":
                return OrderCodingSystem.ABL90_FLEX_PLUS;
            default:
                if (IMG_RESULT_APPS.contains(sendingApplication)) {
                    return OrderCodingSystem.PACS;
                }
        }
        // stripping for DNACPR
        switch (sendingFacility.strip()) {
            case "Vitals":
                return OrderCodingSystem.FLOWSHEET;
            case "Consult Orders":
                return OrderCodingSystem.CONSULT_ORDER;
            case "DNACPR":
                return OrderCodingSystem.ADVANCED_DECISION_ORDER;
            default:
        }

        switch (alternativeIdentifier) {
            case "Profiles":
                return OrderCodingSystem.BANK_MANAGER;
            case "Products":
                return OrderCodingSystem.BLOOD_PRODUCTS;
            default:
        }

        throw new Hl7MessageIgnoredException("Unknown coding system for order/result");
    }

}
