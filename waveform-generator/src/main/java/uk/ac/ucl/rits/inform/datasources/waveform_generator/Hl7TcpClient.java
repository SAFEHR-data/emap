package uk.ac.ucl.rits.inform.datasources.waveform_generator;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;

public class Hl7TcpClient implements AutoCloseable {
    private final Socket socket;

    /**
     * Connect over tcp.
     *
     * @param tcpHost connect to host
     * @param tcpPort connect to port
     * @throws IOException on connect error
     */
    public Hl7TcpClient(String tcpHost, int tcpPort) throws IOException {
        this.socket = new Socket(tcpHost, tcpPort);
    }

    /**
     * Send a message headed with a vertical tab (0b), and
     * terminated with the file separator char (1c).
     *
     * @param msg the byte array of the message to send, without any whitespace header/footer.
     * @throws IOException tcp error
     */
    public void sendMessage(byte[] msg) throws IOException {
        OutputStream os = socket.getOutputStream();
        os.write("\r\u000b".getBytes());
        os.write(msg);
        // There can't be any characters after this in the TCP message, or the
        // Spring Integration TCP server will give errors.
        // I can't yet be sure that this is how Smartlinx behaves!
        os.write("\r\u001c".getBytes());
    }

    /**
     * Close connection.
     * @throws IOException tcp error
     */
    public void close() throws IOException {
        socket.getOutputStream().close();
        socket.close();
    }
}
