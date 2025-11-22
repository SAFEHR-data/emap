package uk.ac.ucl.rits.inform.datasinks.emapstar.controllers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import uk.ac.ucl.rits.inform.datasinks.emapstar.RowState;
import uk.ac.ucl.rits.inform.datasinks.emapstar.exceptions.MessageIgnoredException;
import uk.ac.ucl.rits.inform.datasinks.emapstar.repos.NotesMetadataAuditRepository;
import uk.ac.ucl.rits.inform.datasinks.emapstar.repos.NotesMetadataRepository;


import uk.ac.ucl.rits.inform.informdb.notes.NotesMetadata;
import uk.ac.ucl.rits.inform.informdb.notes.NotesMetadataAudit;
import uk.ac.ucl.rits.inform.informdb.identity.HospitalVisit;
import uk.ac.ucl.rits.inform.interchange.NotesMetadataMessage;

import java.time.Instant;

/**
 * Controller for NotesMetadata specific information.
 * @author Sarah Keating
 */
@Component
public class NotesMetadataController {
    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final NotesMetadataRepository notesMetadataRepository;
    private final NotesMetadataAuditRepository notesMetadataAuditRepository;

    NotesMetadataController(
            NotesMetadataRepository notesMetadataRepository,
            NotesMetadataAuditRepository notesMetadataAuditRepository) {
        this.notesMetadataRepository = notesMetadataRepository;
        this.notesMetadataAuditRepository = notesMetadataAuditRepository;
    }

    /**
     * Process notesMetadata data message.
     * @param msg the interchange message
     * @param visit the hospital visit
     * @param storedFrom stored from timestamp
     * @throws MessageIgnoredException if message not processed
     */
    @Transactional
    public void processMessage(
            NotesMetadataMessage msg,
            HospitalVisit visit,
            Instant storedFrom) throws MessageIgnoredException {

        RowState<NotesMetadata, NotesMetadataAudit> notesMetadataState =      getOrCreateNotesMetadata(msg, visit, storedFrom);

        if (messageShouldBeUpdated(msg.getLastEditDatetime(),     notesMetadataState)) {
            updateNotesMetadata(msg, notesMetadataState);
        }

        notesMetadataState.saveEntityOrAuditLogIfRequired(notesMetadataRepository, notesMetadataAuditRepository);
    }

    /**
     * Get existing or create new advance decision.
     * @param msg                 Advance decision message.
     * @param visit               Hospital visit of patient this advanced decision message refers to.
     * @param storedFrom          Time that emap-core started processing this advanced decision message.
     * @return AdvancedDecision entity wrapped in RowState
     */
    private RowState<NotesMetadata, NotesMetadataAudit> getOrCreateNotesMetadata(NotesMetadataMessage msg, HospitalVisit visit,
        Instant storedFrom) {
        return notesMetadataRepository
            .findByInternalId(msg.getNotesMetadataId())
            .map(obs -> new RowState<>(obs, msg.getLastEditDatetime(), storedFrom, false))
            .orElseGet(() -> createMinimalNotesMetadata(msg, visit, storedFrom));
    }

    /**
     * Create minimal advance decision wrapped in RowState.
     * @param msg                 Advance decision message
     * @param visit               Hospital visit of the patient advanced decision was recorded for.
     * @param storedFrom          Time that emap-core started processing the advanced decision of that patient.
     * @return minimal advanced decision wrapped in RowState
     */
    private RowState<NotesMetadata, NotesMetadataAudit>
        createMinimalNotesMetadata(NotesMetadataMessage msg,
            HospitalVisit visit, Instant storedFrom) {
        NotesMetadata notesMetadata = new NotesMetadata(
            msg.getNotesMetadataId(), visit);
        logger.debug("Created new {}", notesMetadata);
        return new RowState<>(notesMetadata, msg.getLastEditDatetime(), storedFrom, true);
    }

    /**
     * Decides whether or not the data held for a specific advance decision needs to be updated or not.
     * @param statusChangeDatetime  Datetime of NotesMetadataMessage that's currently processed.
     * @param notesMetadataState State of advance decision created from message.
     * @return true if message should be updated
     */
    private boolean messageShouldBeUpdated(Instant statusChangeDatetime, RowState<NotesMetadata,
            NotesMetadataAudit> notesMetadataState) {
        return (notesMetadataState.isEntityCreated() || !statusChangeDatetime.isBefore(
                notesMetadataState.getEntity().getValidFrom()));
    }

    /**
     * Update advance decision data with information from NotesMetadataMessage.
     * @param msg                  Advance decision message.
     * @param notesMetadataState Advance decision referred to in message
     */
    private void updateNotesMetadata(NotesMetadataMessage msg, RowState<NotesMetadata,
            NotesMetadataAudit> notesMetadataState) {
        NotesMetadata notesMetadata = notesMetadataState.getEntity();

        notesMetadataState.assignIfDifferent(msg.getLastEditDatetime(), notesMetadata.getLastEditDatetime(),
                notesMetadata::setLastEditDatetime);
    }
}

