package server.tm;

import common.Error;
import server.utils.Panic;
import server.utils.Parser;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class TransactionManagerImpl implements TransactionManager{

    public static final int XID_HEADER_LENGTH = 8;
    public static final long SUPER_ID = 0;
    private static final int XID_FIELD_LENGTH = 1;

    public static final String XID_SUFFIX = ".xid";

    private static final byte TRAN_ACTIVE = 0;
    private static final byte TRAN_COMMITED = 1;
    private static final byte TRAN_ABORTED = 2;

    private static final long SUPER_XID = 0L;


    private RandomAccessFile raf;
    private FileChannel fc;
    private long xidCounter;
    private Lock counterLock;

    public TransactionManagerImpl(RandomAccessFile raf, FileChannel fc) {
        this.raf = raf;
        this.fc = fc;
        this.counterLock = new ReentrantLock();
        checkXidCounter();
    }

    private void checkXidCounter() {
        long xidFileLength = 0;
        try {
            xidFileLength = raf.length();
        } catch (IOException e) {
            Panic.panic(Error.BadXidFileException());
        }

        if(xidFileLength < XID_HEADER_LENGTH) Panic.panic(Error.BadXidFileException());

        ByteBuffer buf = ByteBuffer.allocate(XID_HEADER_LENGTH);
        try {
            fc.position(0);
            fc.read(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }

        xidCounter = Parser.byte2long(buf.array());
        long end = getXidPosition(xidCounter + 1);
        if(end != xidFileLength) {
            Panic.panic(Error.BadXidFileException());
        }
    }

    private long getXidPosition(long xidCounter) {
        return XID_HEADER_LENGTH + (xidCounter - 1) * XID_FIELD_LENGTH;
    }

    @Override
    public long begin() {
        counterLock.lock();
        try {
            long xid = xidCounter + 1;
            updateXid(xid, TRAN_ACTIVE);
            incrXidCounter();
            return xid;
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            counterLock.unlock();
        }
    }

    private void incrXidCounter() {
        xidCounter++;
        ByteBuffer buf = ByteBuffer.wrap(Parser.long2byte(xidCounter));
        try {
            fc.position(0);
            fc.write(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }
        try {
            fc.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

    private void updateXid(long xid, byte i) {
        byte[] temp = new byte[XID_FIELD_LENGTH];
        temp[0] = i;
        ByteBuffer buf = ByteBuffer.wrap(temp);
        long xidPosition = getXidPosition(xid);
        try {
            fc.position(xidPosition);
            fc.read(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }
        try {
            fc.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

    @Override
    public void commit(long xid) {
        updateXid(xid, TRAN_COMMITED);
    }

    @Override
    public void abort(long xid) {
        updateXid(xid, TRAN_ABORTED);
    }

    @Override
    public boolean isActive(long xid) {
        if(xid == SUPER_XID) return false;
        return checkXid(xid, TRAN_ACTIVE);
    }

    private boolean checkXid(long xid, byte state) {
        ByteBuffer buf = ByteBuffer.wrap(new byte[XID_FIELD_LENGTH]);
        long xidPosition = getXidPosition(xid);
        try {
            fc.position(xidPosition);
            fc.read(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }
        return buf.array()[0] == state;
    }

    @Override
    public boolean isCommitted(long xid) {
        if(xid == SUPER_XID) return true;
        return checkXid(xid, TRAN_COMMITED);
    }

    @Override
    public boolean isAborted(long xid) {
        if (xid == SUPER_XID) return false;
        return checkXid(xid, TRAN_ABORTED);
    }

    @Override
    public void close() {
        try {
            raf.close();
            fc.close();
        } catch (IOException e) {
            Panic.panic(e);
        }
    }
}
