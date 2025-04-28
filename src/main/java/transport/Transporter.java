package transport;

import java.io.*;
import java.net.Socket;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;

public class Transporter {
    private Socket socket;
    private BufferedReader reader;
    private BufferedWriter writer;

    public Transporter(Socket socket) throws IOException {
        this.socket = socket;
        this.reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        this.writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
    }

    public void send(byte[] data) throws IOException {
        String raw = hexEncode(data);
        writer.write(raw);
        writer.flush();
    }

    public byte[] receive() throws IOException {
        String line = reader.readLine();
        if (line == null) {
            close();
        }
        return hexDecode(line);
    }

    public void close() throws IOException {
        writer.close();
        reader.close();
        socket.close();
    }

    private byte[] hexDecode(String data) throws DecoderException {
        return Hex.decodeHex(data);
    }

    private String hexEncode(byte[] data) {
        return Hex.encodeHexString(data, true) + "\n";
    }
}
