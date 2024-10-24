package uk.ac.ucl.rits.inform.datasources.waveform_generator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Hl7TcpClientPool implements AutoCloseable {
    private final Logger logger = LoggerFactory.getLogger(Hl7TcpClientPool.class);
    private final List<Hl7TcpClient> tcpClients = new ArrayList<>();
    private int nextClient = 0;

    /**
     * Connect over tcp.
     *
     * @param tcpHost connect to host
     * @param tcpPort connect to port
     * @param poolSize number of tcp clients to create
     * @throws IOException on connect error
     */
    public Hl7TcpClientPool(String tcpHost, int tcpPort, int poolSize) throws IOException {
        for (int i = 0; i < poolSize; i++) {
            tcpClients.add(new Hl7TcpClient(tcpHost, tcpPort));
        }
    }

    /**
     * Send a message using the pool.
     *
     * @param msg the byte array of the message to send
     * @throws IOException tcp error
     */
    public void sendMessage(byte[] msg) throws IOException {
        Hl7TcpClient hl7TcpClient = tcpClients.get(nextClient);
        hl7TcpClient.sendMessage(msg);
        nextClient = (nextClient + 1) % tcpClients.size();
    }

    /**
     * Close connection.
     * @throws IOException tcp error
     */
    public void close() throws IOException {
        for (var c: tcpClients) {
            c.close();
        }
    }
}
