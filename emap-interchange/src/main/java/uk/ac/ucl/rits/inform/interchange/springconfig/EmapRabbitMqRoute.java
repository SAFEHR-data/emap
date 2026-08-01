package uk.ac.ucl.rits.inform.interchange.springconfig;

/**
 * All the exchanges/queues that sources can use to write into the core processor's message queue.
 * @param queueName name of queue
 * @param exchangeName name of exchange
 *
 * @author Jeremy Stein
 */
public record EmapRabbitMqRoute(EmapDataSourceQueue queueName, EmapDataSourceExchange exchangeName) {
    public enum EmapDataSourceQueue {
        /**
         * The message queue derived from the HL7 ADT (IDS) feed.
         * Bit of a misnomer now that there are other HL7 inputs to
         * Emap (Waveform HL7s), and of course this queue never
         * contained HL7 messages anyway.
         */
        HL7_QUEUE("hl7Queue"),
        /**
         * The message queue for receiving waveform data into emap.
         */
        WAVEFORM_DATA("waveform_emap"),
        /**
         * For when we don't want to specify a queue.
         */
        IGNORED_QUEUE(""),
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
