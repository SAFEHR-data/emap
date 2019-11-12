package uk.ac.ucl.rits.inform.tests;

import java.io.IOException;

import org.junit.Rule;
import org.junit.rules.ExpectedException;

import ca.uhn.hl7v2.HL7Exception;
import uk.ac.ucl.rits.inform.datasinks.emapstar.exceptions.MessageIgnoredException;
import uk.ac.ucl.rits.inform.datasources.ids.exceptions.Hl7InconsistencyException;
import uk.ac.ucl.rits.inform.interchange.EmapOperationMessageProcessingException;

/**
 * If we get the merge message twice, the second message should be ignored and
 * the records should stay merged.
 *
 * @author Jeremy Stein
 */
public class TestMergeByIdIdempotence extends TestMergeById {
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    public TestMergeByIdIdempotence() {
        // do the merge a second time, should get a MessageIgnoredException
        hl7StreamFileNames.add("GenericAdt/A40.txt");
    }

    @Override
    public void setup()
            throws IOException, HL7Exception, Hl7InconsistencyException, EmapOperationMessageProcessingException {
        thrown.expect(MessageIgnoredException.class);
        super.setup();
    }
}
