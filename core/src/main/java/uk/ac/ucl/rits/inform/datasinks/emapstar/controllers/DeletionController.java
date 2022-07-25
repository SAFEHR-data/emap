package uk.ac.ucl.rits.inform.datasinks.emapstar.controllers;

import org.springframework.stereotype.Component;
import uk.ac.ucl.rits.inform.datasinks.emapstar.repos.ConsultationRequestAuditRepository;
import uk.ac.ucl.rits.inform.datasinks.emapstar.repos.ConsultationRequestRepository;
import uk.ac.ucl.rits.inform.datasinks.emapstar.repos.FormAnswerAuditRepository;
import uk.ac.ucl.rits.inform.datasinks.emapstar.repos.FormAnswerRepository;
import uk.ac.ucl.rits.inform.datasinks.emapstar.repos.FormAuditRepository;
import uk.ac.ucl.rits.inform.datasinks.emapstar.repos.FormRepository;
import uk.ac.ucl.rits.inform.datasinks.emapstar.repos.HospitalVisitAuditRepository;
import uk.ac.ucl.rits.inform.datasinks.emapstar.repos.HospitalVisitRepository;
import uk.ac.ucl.rits.inform.datasinks.emapstar.repos.labs.LabOrderAuditRepository;
import uk.ac.ucl.rits.inform.datasinks.emapstar.repos.labs.LabOrderRepository;
import uk.ac.ucl.rits.inform.datasinks.emapstar.repos.labs.LabResultAuditRepository;
import uk.ac.ucl.rits.inform.datasinks.emapstar.repos.labs.LabResultRepository;
import uk.ac.ucl.rits.inform.informdb.consults.ConsultationRequest;
import uk.ac.ucl.rits.inform.informdb.consults.ConsultationRequestAudit;
import uk.ac.ucl.rits.inform.informdb.forms.Form;
import uk.ac.ucl.rits.inform.informdb.forms.FormAnswer;
import uk.ac.ucl.rits.inform.informdb.identity.HospitalVisit;
import uk.ac.ucl.rits.inform.informdb.identity.HospitalVisitAudit;
import uk.ac.ucl.rits.inform.informdb.identity.Mrn;
import uk.ac.ucl.rits.inform.informdb.labs.LabOrder;
import uk.ac.ucl.rits.inform.informdb.labs.LabOrderAudit;
import uk.ac.ucl.rits.inform.informdb.labs.LabResult;
import uk.ac.ucl.rits.inform.informdb.labs.LabResultAudit;

import java.time.Instant;
import java.util.List;

/**
 * @author Jeremy Stein
 * <p>
 * Operations to delete entities in a cascading fashion, creating new audit rows as appropriate.
 * This was written with performing unfiltered, cascading deletes in mind
 * eg. delete an entire person's record.
 * It could be adapted to be more selective (but still be cascading).
 */
@Component
public class DeletionController {
    private final LabOrderRepository labOrderRepo;
    private final LabOrderAuditRepository labOrderAuditRepo;
    private final LabResultRepository labResultRepo;
    private final LabResultAuditRepository labResultAuditRepo;
    private final ConsultationRequestRepository consultationRequestRepo;
    private final ConsultationRequestAuditRepository consultationRequestAuditRepo;
    private final HospitalVisitRepository hospitalVisitRepo;
    private final HospitalVisitAuditRepository hospitalVisitAuditRepo;
    private final FormRepository formRepository;
    private final FormAuditRepository formAuditRepository;
    private final FormAnswerRepository formAnswerRepository;
    private final FormAnswerAuditRepository formAnswerAuditRepository;

    /**
     * Deletion controller needs access to pretty much every repo in order to do cascading deletes.
     * @param labOrderRepo                 lab order repo
     * @param labOrderAuditRepo            lab order audit repo
     * @param labResultRepo                lab result repo
     * @param labResultAuditRepo           lab result audit repo
     * @param consultationRequestRepo      consultation request repo
     * @param consultationRequestAuditRepo consultation request audit repo
     * @param hospitalVisitRepo            hospital visit repo
     * @param hospitalVisitAuditRepo       hospital visit audit repo
     * @param formAnswerAuditRepository    form answer audit repo
     * @param formAnswerRepository         form answer repo
     * @param formAuditRepository          form audit repo
     * @param formRepository               form repo
     */
    @SuppressWarnings("checkstyle:parameternumber")
    public DeletionController(
            LabOrderRepository labOrderRepo, LabOrderAuditRepository labOrderAuditRepo,
            LabResultRepository labResultRepo, LabResultAuditRepository labResultAuditRepo,
            ConsultationRequestRepository consultationRequestRepo, ConsultationRequestAuditRepository consultationRequestAuditRepo,
            HospitalVisitRepository hospitalVisitRepo, HospitalVisitAuditRepository hospitalVisitAuditRepo,
            FormRepository formRepository, FormAuditRepository formAuditRepository,
            FormAnswerRepository formAnswerRepository, FormAnswerAuditRepository formAnswerAuditRepository
    ) {
        this.labOrderRepo = labOrderRepo;
        this.labOrderAuditRepo = labOrderAuditRepo;
        this.labResultRepo = labResultRepo;
        this.labResultAuditRepo = labResultAuditRepo;
        this.consultationRequestRepo = consultationRequestRepo;
        this.consultationRequestAuditRepo = consultationRequestAuditRepo;
        this.hospitalVisitRepo = hospitalVisitRepo;
        this.hospitalVisitAuditRepo = hospitalVisitAuditRepo;
        this.formRepository = formRepository;
        this.formAuditRepository = formAuditRepository;
        this.formAnswerRepository = formAnswerRepository;
        this.formAnswerAuditRepository = formAnswerAuditRepository;
    }

    /**
     * Delete all visits that are older than the current message, along with tables which require visits.
     * @param visits           List of hopsital visits
     * @param invalidationTime Time of the delete information message
     * @param deletionTime     time that emap-core started processing the message.
     */
    public void deleteVisitsAndDependentEntities(Iterable<HospitalVisit> visits, Instant invalidationTime, Instant deletionTime) {
        for (HospitalVisit visit : visits) {
            deleteLabOrdersForVisit(visit, invalidationTime, deletionTime);
            deleteConsultRequestsForVisit(visit, invalidationTime, deletionTime);
            deleteFormsForVisit(visit, invalidationTime, deletionTime);

            hospitalVisitAuditRepo.save(new HospitalVisitAudit(visit, invalidationTime, deletionTime));
            hospitalVisitRepo.delete(visit);
        }
    }

    private void deleteFormsForVisit(HospitalVisit visit, Instant invalidationTime, Instant deletionTime) {
        List<Form> allFormsForVisit = formRepository.findAllByHospitalVisitId(visit);
        deleteForms(allFormsForVisit, invalidationTime, deletionTime);
    }

    /**
     * Delete all forms and form answers directly attached to an MRN.
     * @param mrn mrn to delete from
     * @param invalidationTime invalidation time
     * @param deletionTime deletion time
     */
    public void deleteFormsForMrn(Mrn mrn, Instant invalidationTime, Instant deletionTime) {
        List<Form> allFormsForMrn = formRepository.findAllByMrnId(mrn);
        deleteForms(allFormsForMrn, invalidationTime, deletionTime);
    }


    private void deleteForms(List<Form> allFormsForVisit, Instant invalidationTime, Instant deletionTime) {
        for (Form form: allFormsForVisit) {
            List<FormAnswer> formAnswers = form.getFormAnswers();
            for (FormAnswer ans : formAnswers) {
                formAnswerAuditRepository.save(ans.createAuditEntity(invalidationTime, deletionTime));
            }
            formAnswerRepository.deleteAll(formAnswers);
            formAuditRepository.save(form.createAuditEntity(invalidationTime, deletionTime));
        }
        formRepository.deleteAll(allFormsForVisit);
    }

    private void deleteLabOrdersForVisit(HospitalVisit visit, Instant invalidationTime, Instant deletionTime) {
        List<LabOrder> labOrders = labOrderRepo.findAllByHospitalVisitId(visit);
        for (var lo : labOrders) {
            deleteLabResultsForLabOrder(lo, invalidationTime, deletionTime);

            LabOrderAudit labOrderAudit = lo.createAuditEntity(invalidationTime, deletionTime);
            labOrderAuditRepo.save(labOrderAudit);
            labOrderRepo.delete(lo);
        }

    }

    private void deleteLabResultsForLabOrder(LabOrder labOrder, Instant invalidationTime, Instant deletionTime) {
        List<LabResult> labResults = labResultRepo.findAllByLabOrderId(labOrder);
        for (var lr : labResults) {
            LabResultAudit resultAudit = lr.createAuditEntity(invalidationTime, deletionTime);
            labResultAuditRepo.save(resultAudit);
            labResultRepo.delete(lr);
        }
    }

    private void deleteConsultRequestsForVisit(HospitalVisit visit, Instant invalidationTime, Instant deletionTime) {
        List<ConsultationRequest> consultationRequests = consultationRequestRepo.findAllByHospitalVisitId(visit);
        for (var cr : consultationRequests) {
            ConsultationRequestAudit auditEntity = cr.createAuditEntity(invalidationTime, deletionTime);
            consultationRequestAuditRepo.save(auditEntity);
            consultationRequestRepo.delete(cr);
        }
    }

}
