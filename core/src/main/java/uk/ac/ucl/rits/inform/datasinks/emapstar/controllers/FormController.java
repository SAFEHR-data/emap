package uk.ac.ucl.rits.inform.datasinks.emapstar.controllers;

import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import uk.ac.ucl.rits.inform.datasinks.emapstar.RowState;
import uk.ac.ucl.rits.inform.datasinks.emapstar.repos.forms.FormAnswerAuditRepository;
import uk.ac.ucl.rits.inform.datasinks.emapstar.repos.forms.FormAnswerRepository;
import uk.ac.ucl.rits.inform.datasinks.emapstar.repos.forms.FormAuditRepository;
import uk.ac.ucl.rits.inform.datasinks.emapstar.repos.forms.FormDefinitionAuditRepository;
import uk.ac.ucl.rits.inform.datasinks.emapstar.repos.forms.FormDefinitionRepository;
import uk.ac.ucl.rits.inform.datasinks.emapstar.repos.forms.FormQuestionAuditRepository;
import uk.ac.ucl.rits.inform.datasinks.emapstar.repos.forms.FormQuestionRepository;
import uk.ac.ucl.rits.inform.datasinks.emapstar.repos.forms.FormRepository;
import uk.ac.ucl.rits.inform.informdb.TemporalFrom;
import uk.ac.ucl.rits.inform.informdb.forms.Form;
import uk.ac.ucl.rits.inform.informdb.forms.FormAnswer;
import uk.ac.ucl.rits.inform.informdb.forms.FormAnswerAudit;
import uk.ac.ucl.rits.inform.informdb.forms.FormDefinition;
import uk.ac.ucl.rits.inform.informdb.forms.FormDefinitionAudit;
import uk.ac.ucl.rits.inform.informdb.forms.FormQuestion;
import uk.ac.ucl.rits.inform.informdb.forms.FormQuestionAudit;
import uk.ac.ucl.rits.inform.informdb.identity.HospitalVisit;
import uk.ac.ucl.rits.inform.informdb.identity.Mrn;
import uk.ac.ucl.rits.inform.interchange.form.FormAnswerMsg;
import uk.ac.ucl.rits.inform.interchange.form.FormMetadataMsg;
import uk.ac.ucl.rits.inform.interchange.form.FormMsg;
import uk.ac.ucl.rits.inform.interchange.form.FormQuestionMetadataMsg;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Processing of Forms, AKA Smart Data elements.
 * <p>
 * The Form definition has been broken out into a separate component to make the deliniation of metadata and patient data more clear.
 * @author Jeremy Stein
 * @author Stef Piatek
 */
@Component
@Transactional
public class FormController {
    private final FormRepository formRepository;
    private final FormAuditRepository formAuditRepository;
    private final FormAnswerRepository formAnswerRepository;
    private final FormAnswerAuditRepository formAnswerAuditRepository;
    private final FormDefinitionController formDefinitionController;

    /**
     * @param formRepository            to store the instance of a form
     * @param formAuditRepository       to store deletions of forms
     * @param formAnswerRepository      to store the answers from a form
     * @param formAnswerAuditRepository to audit changes to form answers
     * @param formDefinitionController  to process form and question definitions
     */
    public FormController(
            FormRepository formRepository,
            FormAuditRepository formAuditRepository, FormAnswerRepository formAnswerRepository,
            FormAnswerAuditRepository formAnswerAuditRepository, FormDefinitionController formDefinitionController) {
        this.formRepository = formRepository;
        this.formAuditRepository = formAuditRepository;
        this.formAnswerRepository = formAnswerRepository;
        this.formAnswerAuditRepository = formAnswerAuditRepository;
        this.formDefinitionController = formDefinitionController;
    }

    /**
     * Delete all form data for a given hospital visit.
     * @param visit            hospital visit to query on
     * @param invalidationTime Time of the delete information message
     * @param deletionTime     time that emap-core started processing the message.
     */
    public void deleteFormsForVisit(HospitalVisit visit, Instant invalidationTime, Instant deletionTime) {
        List<Form> allFormsForVisit = formRepository.findAllByHospitalVisitId(visit);
        deleteForms(allFormsForVisit, invalidationTime, deletionTime);
    }

    /**
     * Delete all forms and form answers directly attached to an MRN.
     * @param mrn              mrn to delete from
     * @param invalidationTime Time of the delete information message
     * @param deletionTime     time that emap-core started processing the message.
     */
    public void deleteFormsForMrn(Mrn mrn, Instant invalidationTime, Instant deletionTime) {
        List<Form> allFormsForMrn = formRepository.findAllByMrnIdAndValidFromBefore(mrn, invalidationTime);
        deleteForms(allFormsForMrn, invalidationTime, deletionTime);
    }


    private void deleteForms(Iterable<Form> allFormsForVisit, Instant invalidationTime, Instant deletionTime) {
        for (Form form : allFormsForVisit) {
            List<FormAnswer> formAnswers = form.getFormAnswers();
            for (FormAnswer ans : formAnswers) {
                formAnswerAuditRepository.save(ans.createAuditEntity(invalidationTime, deletionTime));
            }
            formAnswerRepository.deleteAll(formAnswers);
            formAuditRepository.save(form.createAuditEntity(invalidationTime, deletionTime));
        }
        formRepository.deleteAll(allFormsForVisit);
    }

    /**
     * A new (instance of a) form has been completed.
     * @param formMsg       the form message, containing all answers
     * @param storedFrom    stored from timestamp
     * @param hospitalVisit the hospital visit to associate this form with
     */
    public void processForm(FormMsg formMsg, Instant storedFrom, HospitalVisit hospitalVisit) {
        /* This value will only be used if formMsg contains a reference to a form or question
         * that we don't have metadata for so a placeholder metadata entry needs to be created.
         */
        Instant metadataValidFrom = formMsg.getFirstFiledDatetime();

        // get existing or create a new, minimal, metadata entry
        RowState<FormDefinition, FormDefinitionAudit> formDefinition = formDefinitionController.getOrCreateFormDefinition(
                formMsg.getFormId(), storedFrom, metadataValidFrom);

        /* Use the form instance ID to see if the incoming message is referring to an existing form that needs updating.
         * The concept of a form instance doesn't seem to exist in our Epic source data, so that ID will be
         * synthetic, which doesn't matter as long as it's consistent. Not the core processor's problem!
         */
        Form newOrExistingForm = getOrCreateForm(
                new TemporalFrom(formMsg.getFirstFiledDatetime(), storedFrom),
                formDefinition.getEntity(),
                hospitalVisit,
                formMsg.getFormInstanceId(),
                formMsg.getFirstFiledDatetime());

        /* For each answer in the form, determine if it's an update to an existing answer or a new answer in need of adding.
         * If the form is new, all form answers will be new.
         * If the form is pre-existing, there could easily be a mixture of adds and updates (imagine a No->Yes answer change
         * that enables a bunch of previously hidden questions).
         */
        Map<String, FormAnswer> preExistingFormAnswers = newOrExistingForm.getFormAnswers().stream().collect(
                Collectors.toUnmodifiableMap(fa -> fa.getFormQuestionId().getInternalId(), Function.identity()));
        for (FormAnswerMsg answerMsg : formMsg.getFormAnswerMsgs()) {
            FormAnswer formAnswer = preExistingFormAnswers.get(answerMsg.getQuestionId());
            boolean entityJustCreated = formAnswer == null;
            if (entityJustCreated) {
                RowState<FormQuestion, FormQuestionAudit> formQuestion = formDefinitionController.getOrCreateFormQuestion(
                        answerMsg.getQuestionId(), storedFrom, metadataValidFrom);
                formAnswer = new FormAnswer(
                        new TemporalFrom(formMsg.getFirstFiledDatetime(), storedFrom),
                        newOrExistingForm,
                        formQuestion.getEntity());
                formAnswer.setInternalId(answerMsg.getSourceMessageId());
            }
            RowState<FormAnswer, FormAnswerAudit> formAnswerRowState = new RowState<>(formAnswer, metadataValidFrom, storedFrom, entityJustCreated);
            setValuesForAllTypes(formAnswerRowState, formAnswer, answerMsg);
            formAnswerRowState.assignIfDifferent(answerMsg.getFiledDatetime(), formAnswer.getFiledDatetime(), formAnswer::setFiledDatetime);
            formAnswerRowState.saveEntityOrAuditLogIfRequired(formAnswerRepository, formAnswerAuditRepository);
        }
    }

    private Form getOrCreateForm(TemporalFrom temporalFrom, FormDefinition formDefinition, HospitalVisit hospitalVisit,
                                 String formInstanceId, Instant firstFiledDatetime) {
        return formRepository.findByInternalId(formInstanceId).orElseGet(
                () -> formRepository.save(new Form(temporalFrom, formDefinition, hospitalVisit, formInstanceId, firstFiledDatetime)));
    }

    private void setValuesForAllTypes(RowState<FormAnswer, FormAnswerAudit> formAnswerRowState, FormAnswer formAnswer, FormAnswerMsg answerMsg) {
        if (answerMsg.getStringValue().isSave()) {
            formAnswerRowState.assignIfDifferent(
                    answerMsg.getStringValue().get(),
                    formAnswer.getValueAsText(),
                    formAnswer::setValueAsText);
        }
        if (answerMsg.getBooleanValue().isSave()) {
            formAnswerRowState.assignIfDifferent(
                    answerMsg.getBooleanValue().get(),
                    formAnswer.getValueAsBoolean(),
                    formAnswer::setValueAsBoolean);
        }
        if (answerMsg.getDateValue().isSave()) {
            formAnswerRowState.assignIfDifferent(
                    answerMsg.getDateValue().get(),
                    formAnswer.getValueAsDate(),
                    formAnswer::setValueAsDate);
        }
        if (answerMsg.getUtcDatetimeValue().isSave()) {
            formAnswerRowState.assignIfDifferent(
                    answerMsg.getUtcDatetimeValue().get(),
                    formAnswer.getValueAsDatetime(),
                    formAnswer::setValueAsDatetime);
        }
        if (answerMsg.getNumericValue().isSave()) {
            formAnswerRowState.assignIfDifferent(
                    answerMsg.getNumericValue().get(),
                    formAnswer.getValueAsNumber(),
                    formAnswer::setValueAsNumber);
        }
    }

    /**
     * Create new, or update existing form metadata entry.
     * @param formMetadataMsg metadata to update with
     * @param storedFrom      stored from if new rows are created
     */
    public void createOrUpdateFormMetadata(FormMetadataMsg formMetadataMsg, Instant storedFrom) {
        Instant validFrom = formMetadataMsg.getValidFrom();
        formDefinitionController.createOrUpdateFormMetadata(formMetadataMsg, validFrom, storedFrom);
    }

    /**
     * Create new, or update an existing question metadata entry.
     * @param formQuestionMetadataMsg metadata message
     * @param storedFrom              stored from if new rows are created
     */
    public void createOrUpdateFormQuestionMetadata(FormQuestionMetadataMsg formQuestionMetadataMsg, Instant storedFrom) {
        Instant validFrom = formQuestionMetadataMsg.getValidFrom();
        formDefinitionController.createOrUpdateFormMetadata(formQuestionMetadataMsg, validFrom, storedFrom);
    }
}

/**
 * Controls interaction with the tables for form and question definition.
 * @author Stef Piatek
 */
@Component
class FormDefinitionController {
    private final FormDefinitionRepository formDefinitionRepository;
    private final FormDefinitionAuditRepository formDefinitionAuditRepository;
    private final FormQuestionRepository formQuestionRepository;
    private final FormQuestionAuditRepository formQuestionAuditRepository;

    /**
     * Component to handle form and question definitions.
     * @param formDefinitionRepository      to store the definition of a form
     * @param formDefinitionAuditRepository to audit the changes to the form definition
     * @param formQuestionRepository        to store the questions in a form
     * @param formQuestionAuditRepository   to audit the changes to the form questions
     */
    FormDefinitionController(FormDefinitionRepository formDefinitionRepository, FormDefinitionAuditRepository formDefinitionAuditRepository,
                             FormQuestionRepository formQuestionRepository, FormQuestionAuditRepository formQuestionAuditRepository) {
        this.formDefinitionRepository = formDefinitionRepository;
        this.formDefinitionAuditRepository = formDefinitionAuditRepository;
        this.formQuestionRepository = formQuestionRepository;
        this.formQuestionAuditRepository = formQuestionAuditRepository;
    }

    void createOrUpdateFormMetadata(FormQuestionMetadataMsg formQuestionMetadataMsg, Instant validFrom, Instant storedFrom) {
        RowState<FormQuestion, FormQuestionAudit> formQuestion = getOrCreateFormQuestion(
                formQuestionMetadataMsg.getSourceMessageId(), storedFrom, validFrom);
        formQuestion.assignIfDifferent(
                formQuestionMetadataMsg.getName(),
                formQuestion.getEntity().getConceptName(),
                formQuestion.getEntity()::setConceptName);
        formQuestion.assignIfDifferent(
                formQuestionMetadataMsg.getAbbrevName(),
                formQuestion.getEntity().getConceptAbbrevName(),
                formQuestion.getEntity()::setConceptAbbrevName);
        formQuestion.assignIfDifferent(
                formQuestionMetadataMsg.getDescription(),
                formQuestion.getEntity().getDescription(),
                formQuestion.getEntity()::setDescription);
        formQuestion.saveEntityOrAuditLogIfRequired(formQuestionRepository, formQuestionAuditRepository);
    }

    RowState<FormQuestion, FormQuestionAudit> getOrCreateFormQuestion(String formQuestionId, Instant storedFrom, Instant validFrom) {
        return formQuestionRepository.findByInternalId(formQuestionId)
                .map(fq -> new RowState<>(fq, validFrom, storedFrom, false))
                .orElseGet(() -> {
                    FormQuestion newFq = formQuestionRepository.save(new FormQuestion(new TemporalFrom(validFrom, storedFrom), formQuestionId));
                    return new RowState<>(newFq, validFrom, storedFrom, true);
                });
    }

    void createOrUpdateFormMetadata(FormMetadataMsg formMetadataMsg, Instant validFrom, Instant storedFrom) {
        RowState<FormDefinition, FormDefinitionAudit> formDefinition = getOrCreateFormDefinition(
                formMetadataMsg.getSourceMessageId(), storedFrom, validFrom);
        formDefinition.assignIfDifferent(
                formMetadataMsg.getFormName(),
                formDefinition.getEntity().getName(),
                formDefinition.getEntity()::setName);
        formDefinition.assignIfDifferent(
                formMetadataMsg.getFormPatientFriendlyName(),
                formDefinition.getEntity().getPatientFriendlyName(),
                formDefinition.getEntity()::setPatientFriendlyName);
        formDefinition.saveEntityOrAuditLogIfRequired(formDefinitionRepository, formDefinitionAuditRepository);
    }


    RowState<FormDefinition, FormDefinitionAudit> getOrCreateFormDefinition(String formSourceId, Instant storedFrom, Instant validFrom) {
        return formDefinitionRepository.findByInternalId(formSourceId)
                .map(fd -> new RowState<>(fd, validFrom, storedFrom, false))
                .orElseGet(() -> {
                    FormDefinition newFd = formDefinitionRepository.save(new FormDefinition(new TemporalFrom(validFrom, storedFrom), formSourceId));
                    return new RowState<>(newFd, validFrom, storedFrom, true);
                });
    }
}
