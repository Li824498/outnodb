package transport;

import java.io.IOException;

public class Packager {
    private Transporter transporter;
    private Encoder encoder;

    public Packager(Transporter transporter, Encoder encoder) {
        this.transporter = transporter;
        this.encoder = encoder;
    }

    public void send(Package pkg) throws IOException {
        byte[] data = encoder.encode(pkg);
        transporter.send(data);
    }

    public Package receive() throws Exception {
        byte[] receive = transporter.receive();
        return encoder.decode(receive);
    }

    public void close() throws IOException {
        transporter.close();
    }
}
