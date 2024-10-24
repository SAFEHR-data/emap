package uk.ac.ucl.rits.inform.interchange.springconfig;

/**
 * All the data sources that can write into the core processor's message queue.
 *
 * @author Jeremy Stein
 */
public enum EmapDataSource {
    /**
     * The message queue from the HL7 (IDS) feed.
     */
    HL7_QUEUE("hl7Queue"),
    /**
     * The message queue for waveform data.
     */
    WAVEFORM_DATA("waveform"),
    /**
     * The message queue for database extracts.
     */
    DATABASE_EXTRACTS("databaseExtracts"),
    /**
     * The message queue for data relating to non-core projects, eg. HOCI.
     */
    EXTENSION_PROJECTS("extensionProjects");

    private final String queueName;

    /**
     * @param queueName name of the AMQP queue for this data source
     */
    EmapDataSource(String queueName) {
        this.queueName = queueName;
    }

    /**
     * @return AMQP queue name
     */
    public String getQueueName() {
        return queueName;
    }
}
