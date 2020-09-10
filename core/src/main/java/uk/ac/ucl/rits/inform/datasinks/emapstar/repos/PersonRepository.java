package uk.ac.ucl.rits.inform.datasinks.emapstar.repos;

import org.springframework.stereotype.Component;
import uk.ac.ucl.rits.inform.informdb.demographics.CoreDemographic;
import uk.ac.ucl.rits.inform.informdb.identity.Mrn;
import uk.ac.ucl.rits.inform.informdb.identity.MrnToLive;
import uk.ac.ucl.rits.inform.interchange.adt.AdtMessage;

import java.time.Instant;
import java.util.Optional;

/**
 * Interactions with patients at the person level: MRN and core demographics.
 * @author Stef Piatek
 */
@Component
public class PersonRepository {
    private MrnRepository mrnRepo;
    private MrnToLiveRepository mrnToLiveRepo;
    private CoreDemographicRepository coreDemographicRepo;

    /**
     * Constructor implicitly autowiring beans.
     * @param mrnRepo             mrnRepo
     * @param mrnToLiveRepo       mrnToLiveRepo
     * @param coreDemographicRepo coreDemographicRepo
     */
    public PersonRepository(MrnRepository mrnRepo, MrnToLiveRepository mrnToLiveRepo, CoreDemographicRepository coreDemographicRepo) {
        this.mrnRepo = mrnRepo;
        this.mrnToLiveRepo = mrnToLiveRepo;
        this.coreDemographicRepo = coreDemographicRepo;
    }

    public Mrn mergeMrns(String originalIdentifier, Mrn survivingMrn, Instant mergeTime) {
        // get original mrn by mrn or nhsNumber
        Optional<Mrn> originalMrn = mrnRepo.getByMrnEqualsOrMrnIsNullAndNhsNumberEquals(originalIdentifier, originalIdentifier);
        //TODO
        //
        return survivingMrn;
    }

    /**
     * Get an existing MRN or create a new one using the data provided.
     * @param mrnString       MRN
     * @param nhsNumber       NHS number
     * @param sourceSystem    source system
     * @param messageDateTime date time of the message
     * @param storedFrom      when the message has been read by emap core
     * @return The live MRN for the patient.
     */
    public Mrn getOrCreateMrn(String mrnString, String nhsNumber, String sourceSystem, Instant messageDateTime, Instant storedFrom) {
        // get existing mrn by mrn or (mrn is null and nhsnumber equals)
        Optional<Mrn> optionalMrn = mrnRepo.getByMrnEqualsOrMrnIsNullAndNhsNumberEquals(mrnString, nhsNumber);
        Mrn mrn;
        if (optionalMrn.isPresent()) {
            // mrn exists, get the live mrn
            Mrn messageMrn = optionalMrn.get();
            mrn = mrnToLiveRepo.getByMrnIdEquals(messageMrn).getLiveMrnId();
        } else {
            // create new mrn and mrn_to_live row
            mrn = createNewLiveMrn(mrnString, nhsNumber, sourceSystem, messageDateTime, storedFrom);
        }
        return mrn;
    }

    public CoreDemographic updateOrCreateDemographics(long mrnId, AdtMessage adtMessage, Instant storedFrom) {
        // TODO
        // create demographics from the adtMessage
        CoreDemographic coreDemographic = new CoreDemographic();
        // get current demographics by mrnId
        // if the demographics are not the same, update the demographics
        return coreDemographic;
    }

    private Mrn createNewLiveMrn(String mrnString, String nhsNumber, String sourceSystem, Instant messageDateTime, Instant storedFrom) {
        Mrn mrn = new Mrn();
        mrn.setMrn(mrnString);
        mrn.setNhsNumber(nhsNumber);
        mrn.setSourceSystem(sourceSystem);
        mrn.setStoredFrom(storedFrom);

        MrnToLive mrnToLive = new MrnToLive();
        mrnToLive.setMrnId(mrn);
        mrnToLive.setLiveMrnId(mrn);
        mrnToLive.setStoredFrom(storedFrom);
        mrnToLive.setValidFrom(messageDateTime);
        mrnToLiveRepo.save(mrnToLive);
        return mrn;
    }
}
