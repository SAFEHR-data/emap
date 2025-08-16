package uk.ac.ucl.rits.inform.datasinks.emapstar.forms;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.jdbc.Sql;
import org.springframework.transaction.annotation.Transactional;
import uk.ac.ucl.rits.inform.datasinks.emapstar.MessageProcessingBase;
import uk.ac.ucl.rits.inform.datasinks.emapstar.repos.forms.FormAnswerAuditRepository;
import uk.ac.ucl.rits.inform.datasinks.emapstar.repos.forms.FormAnswerRepository;
import uk.ac.ucl.rits.inform.datasinks.emapstar.repos.forms.FormAuditRepository;
import uk.ac.ucl.rits.inform.datasinks.emapstar.repos.forms.FormDefinitionAuditRepository;
import uk.ac.ucl.rits.inform.datasinks.emapstar.repos.forms.FormDefinitionRepository;
import uk.ac.ucl.rits.inform.datasinks.emapstar.repos.forms.FormQuestionAuditRepository;
import uk.ac.ucl.rits.inform.datasinks.emapstar.repos.forms.FormQuestionRepository;
import uk.ac.ucl.rits.inform.datasinks.emapstar.repos.forms.FormRepository;
import uk.ac.ucl.rits.inform.informdb.forms.Form;
import uk.ac.ucl.rits.inform.informdb.forms.FormAnswer;
import uk.ac.ucl.rits.inform.informdb.forms.FormDefinition;
import uk.ac.ucl.rits.inform.informdb.forms.FormQuestion;
import uk.ac.ucl.rits.inform.interchange.EmapOperationMessageProcessingException;
import uk.ac.ucl.rits.inform.interchange.form.FormMetadataMsg;
import uk.ac.ucl.rits.inform.interchange.form.FormQuestionMetadataMsg;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * @author Jeremy Stein
 * Test Form and Form metadata processing (derived only from Epic Smart Data Elements at the moment)
 */
@Transactional
public class TestFormProcessing extends MessageProcessingBase {
    @Autowired
    private FormRepository formRepository;
    @Autowired
    private FormDefinitionRepository formDefinitionRepository;
    @Autowired
    private FormQuestionRepository formQuestionRepository;
    @Autowired
    private FormDefinitionAuditRepository formDefinitionAuditRepository;
    @Autowired
    private FormQuestionAuditRepository formQuestionAuditRepository;
    @Autowired
    private FormAuditRepository formAuditRepository;
    @Autowired
    private FormAnswerAuditRepository formAnswerAuditRepository;

    // Counts for initialised test DB (populate_db.sql)
    private final long STARTING_NUM_QUESTIONS = 10;
    private final long STARTING_NUM_FORMS = 1;

    private final Instant FALLBACK_INSTANT = Instant.now();

    @Test
    public void formFollowedByFormMetadata() throws EmapOperationMessageProcessingException, IOException {
        // Forms with form answers. Placeholders are created for form and question metadata.
        _processForms();

        // implied from forms above
        assertEquals(8, formQuestionRepository.count());
        assertEquals(1, formDefinitionRepository.count());
        // no updates yet
        assertEquals(0, formQuestionAuditRepository.count());
        assertEquals(0, formDefinitionAuditRepository.count());

        // pick just one form instance to inspect in more detail
        Map<String, FormAnswer> answersByIdPreMetadata = getAnswersByConceptId("22345677");
        assertEquals(Instant.parse("2018-11-01T15:39:15Z"), answersByIdPreMetadata.get("UCLH#1167").getFormId().getFirstFiledDatetime());
        Set<String> expectedQuestions = Set.of("UCLH#1167", "UCLH#1205", "FAKE#0001", "FAKE#0003", "FAKE#0004", "FAKE#0005", "FAKE#0006", "FAKE#0007");
        assertEquals(expectedQuestions, answersByIdPreMetadata.keySet());

        // updates should have created some audit rows
        assertEquals(0, formAuditRepository.count());
        assertEquals(3, formAnswerAuditRepository.count());
        assertEquals(1, formAnswerAuditRepository.findAllByInternalId("72577").size());
        assertEquals(2, formAnswerAuditRepository.findAllByInternalId("72570").size());

        // check some values
        assertEquals("abcabc", answersByIdPreMetadata.get("UCLH#1167").getValueAsText());
        assertNull(answersByIdPreMetadata.get("UCLH#1167").getValueAsBoolean());

        assertEquals("1", answersByIdPreMetadata.get("UCLH#1205").getValueAsText());
        assertEquals(true, answersByIdPreMetadata.get("UCLH#1205").getValueAsBoolean());

        assertEquals("2021-02-04", answersByIdPreMetadata.get("FAKE#0003").getValueAsText());
        assertEquals(LocalDate.parse("2021-02-04"), answersByIdPreMetadata.get("FAKE#0003").getValueAsDate());

        assertEquals("2020-04-05T01:02:00.00Z", answersByIdPreMetadata.get("FAKE#0004").getValueAsText());
        assertEquals(Instant.parse("2020-04-05T01:02:00.00Z"), answersByIdPreMetadata.get("FAKE#0004").getValueAsDatetime());

        assertEquals("1.01", answersByIdPreMetadata.get("FAKE#0005").getValueAsText());
        assertEquals(1.01, answersByIdPreMetadata.get("FAKE#0005").getValueAsNumber());

        // all question concept names should be unknown because there was no metadata in the form data
        for (FormAnswer fa : answersByIdPreMetadata.values()) {
            assertNull(fa.getFormQuestionId().getConceptName());
        }
        // pick any question to reach the form definition
        FormDefinition formDef = answersByIdPreMetadata.get("UCLH#1167").getFormId().getFormDefinitionId();
        // No metadata yet, so we won't know the form name either
        assertNull(formDef.getName());

        // Metadata contains 2 form definition of which we already knew about one;
        // Metadata also contains 10 questions, of which 9 previously unknown, to add to the 8 previously implied.
        // Hence 2 form definitions and 8 + 9 = 17 questions.
        _processMetadata();
        assertEquals(17, formQuestionRepository.count());
        assertEquals(2, formDefinitionRepository.count());

        // some audit rows should have been created due to the updates
        assertEquals(1, formQuestionAuditRepository.count());
        assertEquals(1, formDefinitionAuditRepository.count());

        _validateFormQuestionDetails();
        _validateFormDefinitionDetails();
    }

    private void _validateFormDefinitionDetails() {
        FormDefinition formDefAfterMetadata  = formDefinitionRepository.findByInternalId("2056").orElseThrow();
        assertEquals("UCLH ADVANCED TEP", formDefAfterMetadata.getName());
        assertEquals("tep patient friendly name", formDefAfterMetadata.getPatientFriendlyName());
        assertEquals(Instant.parse("2019-04-08T10:00:00Z"), formDefAfterMetadata.getValidFrom());
        assertEquals(FALLBACK_INSTANT, formDefinitionRepository.findByInternalId("1234").orElseThrow().getValidFrom());
    }

    private void _validateFormQuestionDetails() {
        // re-fetch and check some of the metadata
        Map<String, FormQuestion> questionsByIdPostMetadata = formQuestionRepository.findAllByInternalIdIn(Set.of("UCLH#1205", "UCLH#1209")).stream().collect(
                Collectors.toUnmodifiableMap(FormQuestion::getInternalId, Function.identity()));
        FormQuestion formQuestionUCLH1205 = questionsByIdPostMetadata.get("UCLH#1205");
        assertNotNull(formQuestionUCLH1205);
        assertEquals("ICU Discussion", formQuestionUCLH1205.getConceptAbbrevName());
        assertEquals("ICU DISCUSSION", formQuestionUCLH1205.getConceptName());
        assertEquals(Instant.parse("2019-04-08T10:00:00Z"), formQuestionUCLH1205.getValidFrom());
        assertEquals("Most SDEs have no description but this one has a two-line description (1/2)\nSecond line of description",
                formQuestionUCLH1205.getDescription());

        FormQuestion formQuestionUCLH1209 = questionsByIdPostMetadata.get("UCLH#1209");
        assertNotNull(formQuestionUCLH1209);
        assertEquals(FALLBACK_INSTANT, formQuestionUCLH1209.getValidFrom());
    }

    @Test
    @Sql("/populate_db.sql")
    public void metadataTestStartingPopulated() throws Exception {
        // STARTING_NUM_QUESTIONS questions in initial DB state, metadata messages contain 10 questions
        // of which 0 are new, 2 form definitions of which 1 is new.
        assertEquals(STARTING_NUM_QUESTIONS, formQuestionRepository.count());
        assertEquals(STARTING_NUM_FORMS, formDefinitionRepository.count());
        _processMetadata();
        assertEquals(STARTING_NUM_QUESTIONS + 0, formQuestionRepository.count());
        assertEquals(STARTING_NUM_FORMS + 1, formDefinitionRepository.count());
    }

    @Test
    public void metadataTestStartingBlank() throws Exception {
        // Empty initial state, so this is a test of adding only.
        // The 10 questions and 2 form definitions in the message should be added.
        _processMetadata();
        assertEquals(10, formQuestionRepository.count());
        assertEquals(2, formDefinitionRepository.count());
    }

    /**
     * Process a test FormMsg and perform some basic checks.
     */
    private void _processForms() throws IOException, EmapOperationMessageProcessingException {
        processMessages(messageFactory.getFormMsgs("forms1.yaml"));
        assertEquals(2, formRepository.count());
    }

    private Map<String, FormAnswer> getAnswersByConceptId(String visitId) {
        List<Form> forms = formRepository.findAllByHospitalVisitIdEncounter(visitId);
        assertEquals(1, forms.size());
        Form onlyForm = forms.get(0);
        FormDefinition formDefinition = onlyForm.getFormDefinitionId();
        assertEquals("2056", formDefinition.getInternalId());
        return onlyForm.getFormAnswers().stream().collect(Collectors.toUnmodifiableMap(
                fa -> fa.getFormQuestionId().getInternalId(), Function.identity()));
    }

    /**
     * Process test FormMetadataMsg and FormQuestionMetadataMsg messages, and perform some basic checks.
     */
    private void _processMetadata() throws EmapOperationMessageProcessingException, IOException {
        List<FormMetadataMsg> formMetadataMsgs = messageFactory.getFormMetadataMsg("form_metadata1.yaml", FALLBACK_INSTANT);
        List<FormQuestionMetadataMsg> formQuestionMetadataMsg = messageFactory.getFormQuestionMetadataMsg("form_question_metadata_full.yaml", FALLBACK_INSTANT);
        processMessages(formMetadataMsgs);
        processMessages(formQuestionMetadataMsg);

    }
}
