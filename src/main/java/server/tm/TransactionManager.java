package server.tm;

import common.Error;
import server.utils.Panic;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public interface TransactionManager {
    public long begin();
    public void commit(long xid);
    public void abort(long xid);
    public boolean isActive(long xid);
    public boolean isCommitted(long xid);
    public boolean isAborted(long xid);
    public void close();


    public static TransactionManagerImpl start(String path) {
        File file = new File(path + TransactionManagerImpl.XID_SUFFIX);

        try {
            if(!file.createNewFile()) {
                Panic.panic(Error.FileExistsException());
            }
        } catch (Exception e) {
            Panic.panic(e);
        }

        if(!file.canRead() || !file.canWrite()) {
            Panic.panic(Error.FileCannotRWException());
        }

        RandomAccessFile raf = null;
        FileChannel fc = null;

        try {
            raf = new RandomAccessFile(file, "rw");
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }
        fc = raf.getChannel();

        ByteBuffer buf = ByteBuffer.wrap(new byte[TransactionManagerImpl.XID_HEADER_LENGTH]);

        try {
            fc.position(0);
            fc.write(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }

        return new TransactionManagerImpl(raf, fc);
    }

    public static TransactionManagerImpl open(String path) {
        File file = new File(path + TransactionManagerImpl.XID_SUFFIX);

        if (!file.exists()) {
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

        return new TransactionManagerImpl(raf, fc);
    }
}
