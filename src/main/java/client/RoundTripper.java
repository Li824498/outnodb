package client;

import transport.Package;
import transport.Packager;

import java.io.IOException;

public class RoundTripper {
    private Packager packager;

    public RoundTripper(Packager packager) {
        this.packager = packager;
    }

    public Package roundTrip(Package pkg) throws Exception {
        packager.send(pkg);
        return packager.receive();
    }

    public void close() throws IOException {
        packager.close();
    }
}
