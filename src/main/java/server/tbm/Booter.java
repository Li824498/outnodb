package server.tbm;

import common.Error;
import server.utils.Panic;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;

public class Booter {
    private static final String BOOTER_SUFFIX = ".bt";
    private static final String BOOTER_TMP_SUFFIX = ".bt_tmp";
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

    public byte[] load() {
        byte[] buf = null;
        try {
            buf = Files.readAllBytes(file.toPath());
        } catch (Exception e) {
            Panic.panic(e);
        }
        return buf;
    }

    public void update(byte[] bytes) {
        File tmp = new File(path + BOOTER_TMP_SUFFIX);
        try {
            if (tmp.createNewFile()) {
                Panic.panic(Error.FileExistsException());
            }
        } catch (Exception e) {
            Panic.panic(e);
        }
        if (!tmp.canRead() || !tmp.canWrite()) {
            Panic.panic(Error.FileCannotRWException());
        }
        try {
            FileOutputStream fileOutputStream = new FileOutputStream(tmp);
            fileOutputStream.write(bytes);
            fileOutputStream.flush();
        } catch (Exception e) {
            Panic.panic(e);
        }
        // 用一个临时文件来实现原子写入（atomic write），确保即使中途挂掉、断电、程序崩溃，文件也不会变成一半内容的状态。
        try {
            Files.move(tmp.toPath(), new File(path + BOOTER_TMP_SUFFIX).toPath(), StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            Panic.panic(e);
        }
        file = new File(path + BOOTER_SUFFIX);
        if (!file.canWrite() || !file.canRead()) {
            Panic.panic(Error.FileNotExistsException());
        }
    }
}
