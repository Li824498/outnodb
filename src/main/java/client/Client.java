package client;

import transport.Package;
import transport.Packager;

import java.io.IOException;

public class Client {
    private RoundTripper rt;

    public Client(Packager packager) {
        this.rt = new RoundTripper(packager);
    }

    public byte[] excute(byte[] stat) throws Exception {
        Package pkg = new Package(stat, null);
        Package resPkg = rt.roundTrip(pkg);
        if (resPkg.getErr() != null) {
            throw resPkg.getErr();
        }
        return resPkg.getData();
    }

    public void close() {
        try {
            rt.close();
        } catch (IOException e) {

        }
    }
}
