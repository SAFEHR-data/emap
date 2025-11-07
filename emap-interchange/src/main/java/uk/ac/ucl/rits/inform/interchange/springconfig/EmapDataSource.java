package uk.ac.ucl.rits.inform.interchange.springconfig;

/**
 * All the data sources that can write into the core processor's message queue.
 * @param queueName name of queue
 * @param exchangeName name of exchange
 *
 * @author Jeremy Stein
 */
public record EmapDataSource(EmapDataSourceQueue queueName, EmapDataSourceExchange exchangeName) {
    public enum EmapDataSourceQueue {
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
        EmapDataSourceQueue(String queueName) {
            this.queueName = queueName;
        }

        /**
         * @return AMQP queue name
         */
        public String getQueueName() {
            return queueName;
        }
    }

    public enum EmapDataSourceExchange {
        /**
         * default exchange.
         */
        DEFAULT_EXCHANGE(""),
        /**
         * waveform exchange (has multiple downstream queues).
         */
        WAVEFORM_EXCHANGE("waveform");

        private final String exchangeName;

        /**
         * @param exchangeName name of the AMQP exchange for this data source
         */
        EmapDataSourceExchange(String exchangeName) {
            this.exchangeName = exchangeName;
        }

        /**
         * @return AMQP queue name
         */
        public String getExchangeName() {
            return exchangeName;
        }
    }
}
