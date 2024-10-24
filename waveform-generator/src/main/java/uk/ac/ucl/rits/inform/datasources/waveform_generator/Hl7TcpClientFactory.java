package uk.ac.ucl.rits.inform.datasources.waveform_generator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Component
public class Hl7TcpClientFactory {
    private final Logger logger = LoggerFactory.getLogger(Hl7TcpClientFactory.class);

    @Value("${waveform.hl7.send_host}")
    private String tcpHost;

    @Value("${waveform.hl7.send_port}")
    private int tcpPort;

    public Hl7TcpClientPool createTcpClientPool(int poolSize) throws IOException {
        return new Hl7TcpClientPool(tcpHost, tcpPort, poolSize);
    }

    public Hl7TcpClient createTcpClient() throws IOException {
        return new Hl7TcpClient(tcpHost, tcpPort);
    }
}
