package server.dm.logger;

import common.Error;
import server.utils.Panic;
import server.utils.Parser;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public interface Logger {
    void log(byte[] data);
    void truncate(long position);
    byte[] next();
    void rewind();
    void close();

    public static LoggerImpl create(String path) {
        File file = new File(path + LoggerImpl.LOG_SUFFIX);
        try {
            if(!file.createNewFile()) {
                Panic.panic(Error.FileExistsException());
            }
        } catch (IOException e) {
            Panic.panic(e);
        }

        if(!file.canRead() || !file.canWrite()) {
            Panic.panic(Error.FileNotExistsException());
        }

        RandomAccessFile raf = null;
        FileChannel fc = null;

        try {
            raf = new RandomAccessFile(file, "rw");
            fc = raf.getChannel();
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }

        ByteBuffer buf = ByteBuffer.wrap(Parser.int2byte(0));
        try {
            fc.position(0);
            fc.write(buf);
            fc.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        }

        return new LoggerImpl(raf, fc, 0);
    }

    public static LoggerImpl open(String path) {
        File file = new File(path + LoggerImpl.LOG_SUFFIX);

        if(!file.exists()) {
            Panic.panic(Error.FileNotExistsException());
        }

        if(!file.canRead() || !file.canWrite()) {
            Panic.panic(Error.FileCannotRWException());
        }

        RandomAccessFile raf = null;
        FileChannel fc = null;

        try {
            raf = new RandomAccessFile(file, "rw");
            fc = raf.getChannel();
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }

        LoggerImpl logger = new LoggerImpl(raf, fc);
        logger.init();

        return logger;
    }
}
