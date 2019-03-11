package uk.ac.ucl.rits.inform;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import org.apache.commons.lang.NotImplementedException;
import org.hibernate.query.Query;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import ca.uhn.hl7v2.HL7Exception;
import ca.uhn.hl7v2.model.Message;
import ca.uhn.hl7v2.model.v27.message.ADT_A01;
import ca.uhn.hl7v2.parser.PipeParser;
import uk.ac.ucl.rits.inform.ids.IdsMaster;
import uk.ac.ucl.rits.inform.informdb.Attribute;
import uk.ac.ucl.rits.inform.informdb.AttributeKeyMap;
import uk.ac.ucl.rits.inform.informdb.AttributeRepository;
import uk.ac.ucl.rits.inform.informdb.Encounter;
import uk.ac.ucl.rits.inform.informdb.EncounterRepository;
import uk.ac.ucl.rits.inform.informdb.Mrn;
import uk.ac.ucl.rits.inform.informdb.MrnRepository;
import uk.ac.ucl.rits.inform.informdb.PatientDemographicFact;
import uk.ac.ucl.rits.inform.informdb.PatientDemographicFactRepository;
import uk.ac.ucl.rits.inform.informdb.PatientDemographicProperty;
import uk.ac.ucl.rits.inform.informdb.Person;
import uk.ac.ucl.rits.inform.informdb.PersonRepository;

@Component
public class DBTester {
    @Autowired
    private AttributeRepository attributeRepository;
    @Autowired
    private PersonRepository personRepo;
    @Autowired
    private MrnRepository mrnRepo;
    @Autowired
    private EncounterRepository encounterRepo;
    @Autowired
    private PatientDemographicFactRepository patientDemographicFactRepository;
    @Autowired
    private IdsProgressRepository idsProgressRepository;

    private final static Logger logger = LoggerFactory.getLogger(DBTester.class);

    private String idsCfgXml;

    private SessionFactory idsFactory;
    private Session idsSession;

    public DBTester(
            @Value("${ids.cfg.xml.file}") String idsCfgXml,
            @Autowired Environment environment
        ) {
        this.idsCfgXml = idsCfgXml;
        System.out.println("DBTester() 1 - opening config file " + idsCfgXml);
        String envPrefix = "IDS";
        if (environment.acceptsProfiles("test")) {
            envPrefix = null;
        }
        idsFactory = makeSessionFactory(idsCfgXml, envPrefix);
        System.out.println("DBTester() 2");
    }

    public void close() {
        if (idsFactory != null) {
            idsFactory.close();
        }
        idsFactory = null;
    }

    /**
     * Wrapper for the entire transaction that performs: - read latest processed ID
     * from Inform-db (ETL metadata) - process the message and write to Inform-db -
     * write the latest processed ID to reflect the above message
     * 
     * @param parser the HAPI parser to be used
     * 
     * @return 1 if a message was processed, 0 if skipped, -1 if there were no messages to
     *         process
     */
    @Transactional(rollbackFor = HL7Exception.class)
    public int processNextHl7(PipeParser parser) throws HL7Exception {
        int lastProcessedId = getLatestProcessedId();

        IdsMaster idsMsg = getNextHL7IdsRecord(lastProcessedId);
        if (idsMsg == null) {
            return -1;
        }
        int processed = 0;
        String hl7msg = idsMsg.getHl7message();
        // HL7 is supposed to use \r for line endings, but
        // the IDS uses \n
        hl7msg = hl7msg.replace("\n", "\r");
        Message msgFromIds = parser.parse(hl7msg);
        String messagetype = "";
        ADT_A01 a01_ish = null;
        if (msgFromIds instanceof ADT_A01) {
            a01_ish = (ADT_A01) msgFromIds;
            // ok, but is it really an A01?
            messagetype = a01_ish.getMSH().getMessageType().getTriggerEvent().getValue();
            System.out.println("message type: " + messagetype);
        }
        if (messagetype.equals("A01")) {
            Encounter enc = addEncounter(new A01Wrap(a01_ish));
            System.out.println("[" + idsMsg.getUnid() + "] Added from IDS: " + enc.toString());
            processed = 1;
        } else {
            System.out.println("[" + idsMsg.getUnid() + "] Skipping " + messagetype + " (" + msgFromIds.getClass() + ")");
        }
        setLatestProcessedId(idsMsg.getUnid());

        return processed;
    }

    @Transactional
    public int getLatestProcessedId() {
        IdsProgress onlyRow = idsProgressRepository.findOnlyRow();
        if (onlyRow == null) {
            onlyRow = new IdsProgress();
            // Is it wrong to set in a get?
            onlyRow = idsProgressRepository.save(onlyRow);
        }
        return onlyRow.getLastProcessedIdsUnid();
    }

    @Transactional
    public void setLatestProcessedId(int lastProcessedIdsUnid) {
        IdsProgress onlyRow = idsProgressRepository.findOnlyRow();
        onlyRow.setLastProcessedIdsUnid(lastProcessedIdsUnid);
        idsProgressRepository.save(onlyRow);
    }

    public String getNextHL7IdsMsg(int lastProcessedId) {
        IdsMaster next = getNextHL7IdsRecord(lastProcessedId);
        if (next == null) {
            return null;
        } else {
            System.out.println("Got message with unid " + next.getUnid());
            return next.getHl7message();
        }
    }

    public IdsMaster getNextHL7IdsRecord(int lastProcessedId) {
        idsSession = idsFactory.openSession();
        Query<IdsMaster> qnext = idsSession.createQuery("from IdsMaster where unid > :lastProcessedId order by unid",
                IdsMaster.class);
        qnext.setParameter("lastProcessedId", lastProcessedId);
        qnext.setMaxResults(1);
        List<IdsMaster> nextMsgOrEmpty = qnext.list();
        idsSession.close();
        if (nextMsgOrEmpty.isEmpty()) {
            return null;
        } else if (nextMsgOrEmpty.size() == 1) {
            return nextMsgOrEmpty.get(0);
        } else {
            throw new InternalError();
        }
    }

    /**
     * create a session factory from the given config file, overwriting
     * configurable values from the environment, if specified
     *
     * @param configFile the hibernate xml config file
     * @param envPrefix the prefix for environment variable names, or null if no
     *        variables should be read from the environment
     *
     * @return the SessionFactory thus created
     */
    private static SessionFactory makeSessionFactory(String configFile, String envPrefix) {
        Configuration cfg = new Configuration().configure(configFile);
        cfg.addAnnotatedClass(IdsMaster.class);

        if (envPrefix != null) {
            // take the username and password out of the environment
            // so the config file can safely go into source control
            String envVarUrl = envPrefix + "_JDBC_URL";
            String envVarUsername = envPrefix + "_USERNAME";
            String envVarPassword = envPrefix + "_PASSWORD";

            String url = System.getenv(envVarUrl);
            String uname = System.getenv(envVarUsername);
            String pword = System.getenv(envVarPassword);
            if (url != null) {
                cfg.setProperty("hibernate.connection.url", url);
            }
            if (uname != null) {
                cfg.setProperty("hibernate.connection.username", uname);
            }
            if (pword != null) {
                cfg.setProperty("hibernate.connection.password", pword);
            }
        }

        return cfg.buildSessionFactory();
    }

    /**
     * Create a new encounter using the details given in the A01 message. This may
     * also entail creating a new Mrn and Person if these don't already exist.
     * 
     * @param encounterDetails
     */
    @Transactional
    public Encounter addEncounter(A01Wrap encounterDetails) {
        String mrnStr = encounterDetails.getMrn();
        Mrn newOrExistingMrn = findOrAddMrn(mrnStr);
        // Encounter is always a new one for an A01
        Encounter enc = new Encounter();
        enc.setStoredFrom(Instant.now());
        enc.setEncounter(encounterDetails.getVisitNumber());
        enc.setValidFrom(encounterDetails.getEventTime());
        enc.setMrn(newOrExistingMrn);
        enc = encounterRepo.save(enc);
        addAttrValue(enc, AttributeKeyMap.FIRST_NAME, encounterDetails.getGivenName());
        addAttrValue(enc, AttributeKeyMap.MIDDLE_NAMES, encounterDetails.getMiddleName());
        addAttrValue(enc, AttributeKeyMap.FAMILY_NAME, encounterDetails.getFamilyName());
        return enc;
    }
    
    /**
     * This is a temporary way to add a single-property fact
     * to an encounter. Will be superceded by Roma's version of the
     * Inform-db JPA entities
     * 
     * @param enc
     * @param attrKM
     * @param factValue
     */
    private void addAttrValue(Encounter enc, AttributeKeyMap attrKM, String factValue) {
        // doesn't consider that the fact may already exist
        PatientDemographicFact fact = new PatientDemographicFact();
        fact.setEncounter(enc);
        Optional<Attribute> attropt = attributeRepository.findByShortName(attrKM.getShortname());
        Attribute attr;
        if (attropt.isPresent()) {
            attr = attropt.get();
        } else {
            // In future we will have a more orderly list of Attributes, but am
            // creating them on the fly for now
            attr = new Attribute();
            attr.setShortName(attrKM.getShortname());
            attr.setDescription(attrKM.toString()); // just assume a description from the name for now
            attr = attributeRepository.save(attr);
        }
        
        PatientDemographicProperty prop = new PatientDemographicProperty();
        prop.setAttribute(attr);
        prop.setValueAsString(factValue);
        
        fact.addProperty(prop);
        fact = patientDemographicFactRepository.save(fact);

    }

    public long countEncounters() {
        return encounterRepo.count();
    }

    // Find an existing Mrn by its string representation, or create a new
    // Mrn record if it doesn't exist.
    private Mrn findOrAddMrn(String mrnStr) {
        List<Mrn> allMrns = mrnRepo.findByMrnString(mrnStr);
        Mrn mrn;
        if (allMrns.isEmpty()) {
            /*
             * If it's a new MRN then assume that it's also a new person (or at least we
             * don't know which person it is yet, and we'll have to wait for the merge
             * before we find out, so we'll have to create a new person for now)
             */
            mrn = new Mrn();
            mrn.setMrn(mrnStr);
            Person pers = new Person();
            pers.setCreateDatetime(Instant.now());
            personRepo.save(pers);
            mrn.setPerson(pers);
        } else if (allMrns.size() > 1) {
            throw new NotImplementedException("Does this even make sense?");
        } else {
            mrn = allMrns.get(0);
        }
        mrn.setStoredFrom(Instant.now());
        mrn = mrnRepo.save(mrn);
        return mrn;
    }

    private Person findOrAddPerson() {
        Optional<Person> pers = personRepo.findById(42);
        if (pers.isPresent()) {
            Person pgot = pers.get();
            System.out.println(pgot.toString());
            return pgot;

        } else {
            Person pnew = personRepo.save(new Person());
            System.out.println(pnew.toString());
            return pnew;
        }
    }

}
