package uk.ac.ucl.rits.inform.datasinks.emapstar;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.jdbc.Sql;
import uk.ac.ucl.rits.inform.datasinks.emapstar.repos.HospitalVisitRepository;
import uk.ac.ucl.rits.inform.datasinks.emapstar.repos.VisitObservationAuditRepository;
import uk.ac.ucl.rits.inform.datasinks.emapstar.repos.VisitObservationRepository;
import uk.ac.ucl.rits.inform.informdb.identity.HospitalVisit;
import uk.ac.ucl.rits.inform.informdb.identity.Mrn;
import uk.ac.ucl.rits.inform.informdb.identity.MrnToLive;
import uk.ac.ucl.rits.inform.informdb.visit_recordings.VisitObservation;
import uk.ac.ucl.rits.inform.informdb.visit_recordings.VisitObservationAudit;
import uk.ac.ucl.rits.inform.interchange.EmapOperationMessageProcessingException;
import uk.ac.ucl.rits.inform.interchange.Flowsheet;
import uk.ac.ucl.rits.inform.interchange.Hl7Value;

import java.util.List;
import java.util.Optional;

class TestFlowsheetProcessing extends MessageProcessingBase {
    private List<Flowsheet> messages;
    @Autowired
    private HospitalVisitRepository hospitalVisitRepository;
    @Autowired
    private VisitObservationRepository visitObservationRepository;
    @Autowired
    private VisitObservationAuditRepository visitObservationAuditRepository;

    private String updateId = "8";
    private String newComment = "patient was running really fast (on a hamster wheel)";
    private String stringDeleteId = "28315";
    private String numericDeleteId = "6";


    @BeforeEach
    void setup() {
        messages = messageFactory.getFlowsheets("hl7.yaml", "0000040");
    }

    /**
     * no existing mrns, so new mrn, mrn_to_live core_demographics, hospital visit and visit observation should be created.
     * @throws EmapOperationMessageProcessingException shouldn't happen
     */
    @Test
    void testCreateNewPatient() throws EmapOperationMessageProcessingException {
        for (Flowsheet msg : messages) {
            processSingleMessage(msg);
        }
        List<Mrn> mrns = getAllMrns();
        Assertions.assertEquals(1, mrns.size());
        Assertions.assertEquals("EPIC", mrns.get(0).getSourceSystem());

        MrnToLive mrnToLive = mrnToLiveRepo.getByMrnIdEquals(mrns.get(0));
        Assertions.assertNotNull(mrnToLive);

        HospitalVisit visit = hospitalVisitRepository.findByEncounter(defaultEncounter).orElseThrow(NullPointerException::new);

        // 7 flowsheets in input file, but one is a delete so only 6 should be created
        List<VisitObservation> observations = visitObservationRepository.findAllByHospitalVisitId(visit);
        Assertions.assertEquals(6, observations.size());
    }

    /**
     * Row already exists before message is encountered, numeric value is different in message, and message is updated more recently
     * Row should have updated value
     * @throws EmapOperationMessageProcessingException shouldn't happen
     */
    @Test
    @Sql("/populate_db.sql")
    void testRowUpdates() throws EmapOperationMessageProcessingException {
        HospitalVisit visit = hospitalVisitRepository.findByEncounter(defaultEncounter).orElseThrow(NullPointerException::new);
        VisitObservation preUpdateObservation = visitObservationRepository
                .findByHospitalVisitIdAndVisitObservationTypeIdIdInApplication(visit, updateId)
                .orElseThrow();

        for (Flowsheet msg : messages) {
            processSingleMessage(msg);
        }

        // value is updated
        VisitObservation updatedObservation = visitObservationRepository
                .findByHospitalVisitIdAndVisitObservationTypeIdIdInApplication(visit, updateId)
                .orElseThrow();
        Assertions.assertNotEquals(preUpdateObservation.getValueAsReal(), updatedObservation.getValueAsReal());
        // comment is updated
        Assertions.assertEquals(newComment, updatedObservation.getComment());

        // audit log for the old value
        VisitObservationAudit audit = visitObservationAuditRepository
                .findByHospitalVisitIdAndVisitObservationTypeIdIdInApplication(visit.getHospitalVisitId(), updateId)
                .orElseThrow();
        Assertions.assertEquals(preUpdateObservation.getValueAsReal(), audit.getValueAsReal());

    }

    /**
     * Message updated time is before the validFrom, so no update should happen
     * @throws EmapOperationMessageProcessingException shouldn't happen
     */
    @Test
    @Sql("/populate_db.sql")
    void testOldUpdateDoesNothing() throws EmapOperationMessageProcessingException {
        HospitalVisit visit = hospitalVisitRepository.findByEncounter(defaultEncounter).orElseThrow(NullPointerException::new);
        VisitObservation preUpdateObservation = visitObservationRepository
                .findByHospitalVisitIdAndVisitObservationTypeIdIdInApplication(visit, updateId)
                .orElseThrow();

        for (Flowsheet msg : messages) {
            msg.setUpdatedTime(past);
            processSingleMessage(msg);
        }

        VisitObservation updatedObservation = visitObservationRepository
                .findByHospitalVisitIdAndVisitObservationTypeIdIdInApplication(visit, updateId)
                .orElseThrow();

        Assertions.assertEquals(preUpdateObservation.getValueAsReal(), updatedObservation.getValueAsReal());
    }

    /**
     * String flowsheet exists before message is encountered, string value is delete in message, and message is newer than db
     * Row should be deleted
     * @throws EmapOperationMessageProcessingException shouldn't happen
     */
    @Test
    @Sql("/populate_db.sql")
    void testStringDeletes() throws EmapOperationMessageProcessingException {
        HospitalVisit visit = hospitalVisitRepository.findByEncounter(defaultEncounter).orElseThrow(NullPointerException::new);
        VisitObservation preDeleteObservation = visitObservationRepository
                .findByHospitalVisitIdAndVisitObservationTypeIdIdInApplication(visit, stringDeleteId)
                .orElseThrow();

        for (Flowsheet msg : messages) {
            processSingleMessage(msg);
        }

        // visit observation now does not exist
        Optional<VisitObservation> deletedObservation = visitObservationRepository
                .findByHospitalVisitIdAndVisitObservationTypeIdIdInApplication(visit, stringDeleteId);
        Assertions.assertTrue(deletedObservation.isEmpty());

        // audit log for the old value
        VisitObservationAudit audit = visitObservationAuditRepository
                .findByHospitalVisitIdAndVisitObservationTypeIdIdInApplication(visit.getHospitalVisitId(), stringDeleteId)
                .orElseThrow();
        Assertions.assertEquals(preDeleteObservation.getValueAsText(), audit.getValueAsText());
    }

    /**
     * Numeric flowsheet exists before message is encountered, numeric value is delete in message, and message is newer than db
     * Row should be deleted
     * @throws EmapOperationMessageProcessingException shouldn't happen
     */
    @Test
    @Sql("/populate_db.sql")
    void testNumericDeletes() throws EmapOperationMessageProcessingException {
        HospitalVisit visit = hospitalVisitRepository.findByEncounter(defaultEncounter).orElseThrow(NullPointerException::new);
        VisitObservation preDeleteObservation = visitObservationRepository
                .findByHospitalVisitIdAndVisitObservationTypeIdIdInApplication(visit, stringDeleteId)
                .orElseThrow();

        // process flowsheet with delete numeric value
        Flowsheet msg = messages.get(1);
        msg.setNumericValue(Hl7Value.delete());
        processSingleMessage(msg);

        // visit observation now does not exist
        Optional<VisitObservation> deletedObservation = visitObservationRepository
                .findByHospitalVisitIdAndVisitObservationTypeIdIdInApplication(visit, numericDeleteId);
        Assertions.assertTrue(deletedObservation.isEmpty());

        // audit log for the old value
        VisitObservationAudit audit = visitObservationAuditRepository
                .findByHospitalVisitIdAndVisitObservationTypeIdIdInApplication(visit.getHospitalVisitId(), numericDeleteId)
                .orElseThrow();
        Assertions.assertEquals(preDeleteObservation.getValueAsText(), audit.getValueAsText());
    }


    /**
     * Message updated time is before the validFrom, so no delete should happen
     * @throws EmapOperationMessageProcessingException shouldn't happen
     */
    @Test
    @Sql("/populate_db.sql")
    void testOldDeleteDoesNothing() throws EmapOperationMessageProcessingException {
        HospitalVisit visit = hospitalVisitRepository.findByEncounter(defaultEncounter).orElseThrow(NullPointerException::new);
        VisitObservation preDeleteObservation = visitObservationRepository
                .findByHospitalVisitIdAndVisitObservationTypeIdIdInApplication(visit, stringDeleteId)
                .orElseThrow();

        for (Flowsheet msg : messages) {
            msg.setUpdatedTime(past);
            processSingleMessage(msg);
        }

        VisitObservation notDeletedObservation = visitObservationRepository
                .findByHospitalVisitIdAndVisitObservationTypeIdIdInApplication(visit, stringDeleteId)
                .orElseThrow();

        Assertions.assertEquals(preDeleteObservation.getValueAsReal(), notDeletedObservation.getValueAsReal());
    }
}
