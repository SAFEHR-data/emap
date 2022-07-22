package uk.ac.ucl.rits.inform.datasinks.emapstar;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.repository.CrudRepository;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.springframework.transaction.annotation.Transactional;
import uk.ac.ucl.rits.inform.datasinks.emapstar.controllers.PersonController;
import uk.ac.ucl.rits.inform.datasinks.emapstar.repos.MrnRepository;
import uk.ac.ucl.rits.inform.datasinks.emapstar.repos.MrnToLiveRepository;
import uk.ac.ucl.rits.inform.informdb.identity.Mrn;
import uk.ac.ucl.rits.inform.interchange.EmapOperationMessage;
import uk.ac.ucl.rits.inform.interchange.EmapOperationMessageProcessingException;
import uk.ac.ucl.rits.inform.interchange.InterchangeMessageFactory;
import uk.ac.ucl.rits.inform.interchange.adt.AdtMessage;
import uk.ac.ucl.rits.inform.interchange.lab.LabMetadataMsg;

import java.time.Instant;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

@SpringJUnitConfig
@SpringBootTest
@AutoConfigureTestDatabase
@ActiveProfiles("test")
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
public abstract class MessageProcessingBase {
    @Autowired
    protected MrnRepository mrnRepo;
    @Autowired
    protected MrnToLiveRepository mrnToLiveRepo;
    @Autowired
    protected PersonController personController;

    @Autowired
    protected InformDbOperations dbOps;

    protected final String defaultMrn = "40800000";
    protected final String defaultEncounter = "123412341234";
    protected final Instant past = Instant.parse("2000-01-01T01:01:01Z");


    protected final InterchangeMessageFactory messageFactory = new InterchangeMessageFactory();


    @Transactional
    protected void processSingleMessage(EmapOperationMessage msg) throws EmapOperationMessageProcessingException {
        msg.processMessage(dbOps);
    }

    @Transactional
    protected void processMessages(List<? extends EmapOperationMessage> msgs) throws EmapOperationMessageProcessingException {
        for (EmapOperationMessage m : msgs) {
            m.processMessage(dbOps);
        }
    }

    protected List<Mrn> getAllMrns() {
        return StreamSupport.stream(mrnRepo.findAll().spliterator(), false).collect(Collectors.toList());
    }

    protected <T extends Object> List<T> getAllEntities(CrudRepository<T, Long> repo) {
        return StreamSupport.stream(repo.findAll().spliterator(), false).collect(Collectors.toList());
    }


    /**
     * Set person and encounter information to match hospital visit 4002
     * @param msg
     * @param <T>
     * @return
     */
    protected <T extends AdtMessage> T setDataForHospitalVisitId4002(T msg) {
        msg.setVisitNumber("1234567890");
        msg.setMrn("60600000");
        msg.setNhsNumber("1111111111");
        return msg;
    }
}
