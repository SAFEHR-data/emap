# Emap Core Processor (`core`)

This service takes messages from a queue and compares this data to the current data in the EMAP database.
Generally, if a message has newer information that is different, then the message will update the database data,
otherwise the message will have no effect. This is important because the HL7 messages can be received out of order.

