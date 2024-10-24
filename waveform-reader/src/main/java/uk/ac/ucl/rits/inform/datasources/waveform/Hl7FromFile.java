package uk.ac.ucl.rits.inform.datasources.waveform;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;
import uk.ac.ucl.rits.inform.datasources.waveform.hl7parse.Hl7ParseException;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

@Component
public class Hl7FromFile {
    private final Logger logger = LoggerFactory.getLogger(Hl7FromFile.class);

    private final ThreadPoolTaskExecutor listenTaskExecutor;
    private final Hl7ParseAndSend hl7ParseAndSend;
    private final File hl7DumpFile;
    static final String MESSAGE_DELIMITER = "\u001c";

    Hl7FromFile(ThreadPoolTaskExecutor listenTaskExecutor,
                Hl7ParseAndSend hl7ParseAndSend,
                @Value("${waveform.hl7.test_dump_file:#{null}}") File hl7DumpFile
                ) {
        this.listenTaskExecutor = listenTaskExecutor;
        this.hl7ParseAndSend = hl7ParseAndSend;
        this.hl7DumpFile = hl7DumpFile;
    }

    List<String> readFromFile(File hl7DumpFile) throws IOException {
        logger.info("Reading test HL7 file {}", hl7DumpFile);
        Scanner scanner = new Scanner(hl7DumpFile);
        scanner.useDelimiter(MESSAGE_DELIMITER);
        List<String> allMessages = new ArrayList<>();
        while (scanner.hasNext()) {
            String nextMessageStr = scanner.next();
            allMessages.add(nextMessageStr);
        }
        return allMessages;
    }

    @Scheduled(fixedRate = Long.MAX_VALUE) // do once only
    void readOnceAndQueueScheduled() throws Hl7ParseException, WaveformCollator.CollationException, IOException {
        if (hl7DumpFile == null) {
            logger.info("No test HL7 file specified");
            return;
        }
        readOnceAndQueue(hl7DumpFile);
        // Not sure how to wait for Publisher to finish, so just sleep for a bit
        try {
            Thread.sleep(10_000);
        } catch (InterruptedException e) {
            logger.warn("Thread was interrupted", e);
        }
        System.exit(0);
    }

    void readOnceAndQueue(File hl7DumpFile) throws Hl7ParseException, WaveformCollator.CollationException, IOException {
        List<String> messages = readFromFile(hl7DumpFile);
        logger.info("Read {} HL7 messages from test dump file", messages.size());
        for (int mi = 0; mi < messages.size(); mi++) {
            hl7ParseAndSend.parseAndQueue(messages.get(mi));
            if (mi % 100 == 0) {
                logger.info("handled {} messages out of {}", mi + 1, messages.size());
            }
        }
        logger.info("Queued {} HL7 messages from test dump file", messages.size());
    }
}
