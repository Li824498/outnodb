package server.tbm;

import common.Error;
import server.utils.Panic;

import java.io.File;
import java.io.IOException;

public class Booter {
    private static final String BOOTER_SUFFIX = ".bt";
    private String path;
    private File file;

    public Booter(String path, File file) {
        this.path = path;
        this.file = file;
    }


    public static Booter create(String path) {
        removeBadTmp(path);
        File file = new File(path + BOOTER_SUFFIX);
        try {
            if (!file.createNewFile()) {
                Panic.panic(Error.FileExistsException());
            }
        } catch (Exception e) {
            Panic.panic(e);
        }
        if (!file.canRead() || !file.canWrite()) {
            Panic.panic(Error.FileCannotRWException());
        }

        return new Booter(path, file);
    }

    public static Booter open(String path) {
        removeBadTmp(path);
        File file = new File(path + BOOTER_SUFFIX);
        if (!file.exists()) {
            Panic.panic(Error.FileNotExistsException());
        }
        if (!file.canRead() || !file.canWrite()) {
            Panic.panic(Error.FileCannotRWException());
        }
        return new Booter(path, file);
    }

    private static void removeBadTmp(String path) {
        new File(path + BOOTER_SUFFIX).delete();
    }
}
