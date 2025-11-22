package uk.ac.ucl.rits.inform.datasinks.emapstar.dataprocessors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import uk.ac.ucl.rits.inform.datasinks.emapstar.controllers.NotesMetadataController;
import uk.ac.ucl.rits.inform.datasinks.emapstar.controllers.PersonController;
import uk.ac.ucl.rits.inform.datasinks.emapstar.controllers.VisitController;

import uk.ac.ucl.rits.inform.informdb.identity.HospitalVisit;
import uk.ac.ucl.rits.inform.informdb.identity.Mrn;

import uk.ac.ucl.rits.inform.interchange.EmapOperationMessageProcessingException;
import uk.ac.ucl.rits.inform.interchange.NotesMetadataMessage;


import java.time.Instant;

/**
 * Handle processing of notes metadata messages.
 * @author Sarah Keating
 */
@Component
public class NotesMetadataProcessor {
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final PersonController personController;
    private final VisitController visitController;
    private final NotesMetadataController notesMetadataController;

    /**
     * Notes metadata controller to identify whether metadata needs to be updated; person controller to identify patient.
     * @param notesMetadataController    notes metadata controller
     * @param personController           person controller
     * @param visitController            hospital visit controller
     */
    public NotesMetadataProcessor(NotesMetadataController notesMetadataController,
            PersonController personController, VisitController visitController) {
        this.personController = personController;
        this.visitController = visitController;
        this.notesMetadataController = notesMetadataController;
    }

    /**
     * Process notes metadata message.
     * @param msg        message
     * @param storedFrom Time the message started to be processed by star
     * @throws EmapOperationMessageProcessingException if message can't be processed.
     */
    @Transactional
    public void processMessage(final NotesMetadataMessage msg, final Instant storedFrom)
            throws EmapOperationMessageProcessingException {
        Instant msgStatusChangeTime = msg.getLastEditDatetime();
        if (msgStatusChangeTime == null) {
            msgStatusChangeTime = storedFrom;
        }

        // retrieve patient to whom message refers to; if MRN not registered, create new patient
        Mrn mrn = personController.getOrCreateOnMrnOnly(msg.getMrn(), null, msg.getSourceSystem(),
                msgStatusChangeTime, storedFrom);
        HospitalVisit visit = visitController.getOrCreateMinimalHospitalVisit(
                msg.getVisitNumber(), mrn, msg.getSourceSystem(), msg.getLastEditDatetime(), storedFrom);
        logger.trace("Processing {}", msg);
        notesMetadataController.processMessage(msg, visit, storedFrom);
    }
}
