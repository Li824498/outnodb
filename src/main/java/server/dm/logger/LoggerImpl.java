package server.dm.logger;

import com.google.common.primitives.Bytes;
import common.Error;
import server.utils.Panic;
import server.utils.Parser;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


/**
 * Log文件结构：[xCheckSum][Log1][Log2][Log3]...[BadTail]
 * Log结构：[size][checkSum][data]
 *
 * checksum计算规则：
 * 1.总的xCheckSum在计算时会计算所有位
 * 2.各log的checkSum只会计算自己的data部分
 */
public class LoggerImpl implements Logger {
    public static final String LOG_SUFFIX = ".log";

    private final static int LEN_LOG_SIZE = 4;
    private static final int LEN_OF_CHECKSUM = 4;
    private static final int SEED = 13331;
    private static final int OF_DATA = 0 + LEN_LOG_SIZE + LEN_OF_CHECKSUM;
    private static final int OF_CHECKSUM = 0 + LEN_LOG_SIZE;


    private RandomAccessFile raf;
    private FileChannel fc;
    private Lock lock;

    private int xCheckSum;
    private long fileSize;
    private long position;

    public LoggerImpl(RandomAccessFile raf, FileChannel fc, int xCheckSum) {
        this.raf = raf;
        this.fc = fc;
        this.xCheckSum = xCheckSum;
        this.lock = new ReentrantLock();
    }

    public LoggerImpl(RandomAccessFile raf, FileChannel fc) {
        this.raf = raf;
        this.fc = fc;
        this.lock = new ReentrantLock();
    }

    public void init() {
        long length = 0L;
        try {
            length = raf.length();
        } catch (IOException e) {
            Panic.panic(e);
        }
        if(length < 4) {
            Panic.panic(Error.BadLogFileException());
        }
        ByteBuffer buf = ByteBuffer.allocate(4);

        try {
            fc.position(0);
            fc.read(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }

        this.fileSize = length;
        this.xCheckSum = buf.getInt();

        checkAndRemoveTail();

    }

    private void checkAndRemoveTail() {
        rewind();
        int xCheck = 0;
        while(true) {
            byte[] next = internNext();
            if(next == null) break;
            xCheck += calcCheck(xCheck, next);
        }

        if(xCheck != xCheckSum) {
            Panic.panic(Error.BadLogFileException());
        }

        try {
            fc.truncate(position);
        } catch (IOException e) {
            Panic.panic(e);
        }
        try {
            raf.seek(position);
        } catch (IOException e) {
            Panic.panic(e);
        }

        rewind();
    }

    private int calcCheck(int xCheck, byte[] next) {
        for(byte b : next) {
            xCheck = xCheck * SEED + b;
        }
        return xCheck;
    }

    private byte[] internNext() {
        if (position + LEN_LOG_SIZE >= fileSize) {
            return null;
        }
        ByteBuffer bufSize = ByteBuffer.allocate(LEN_LOG_SIZE);
        try {
            fc.position(position);
            fc.read(bufSize);
        } catch (IOException e) {
            Panic.panic(e);
        }
        int size = bufSize.getInt();

        if (position + LEN_LOG_SIZE + LEN_OF_CHECKSUM + size > fileSize) {
            return null;
        }

        ByteBuffer bufSizeCheckData = ByteBuffer.allocate(LEN_LOG_SIZE + LEN_OF_CHECKSUM + size);
        try {
            fc.position(position);
            fc.read(bufSizeCheckData);
        } catch (IOException e) {
            Panic.panic(e);
        }

        byte[] logArray = bufSizeCheckData.array();
        int checkSum1 = calcCheck(0, Arrays.copyOfRange(logArray, OF_DATA, logArray.length));
        int checkSum2 = ByteBuffer.wrap(Arrays.copyOfRange(logArray, OF_CHECKSUM, OF_DATA)).getInt();
        if(checkSum1 != checkSum2) {
            Panic.panic(Error.BadLogFileException());
        }

        position += logArray.length;
        return logArray;
    }

    @Override
    public void log(byte[] data) {
        byte[] size = Parser.int2byte(data.length);
        byte[] checkSum = Parser.int2byte(calcCheck(0, data));
        byte[] logBytes = Bytes.concat(size, checkSum, data);

        lock.lock();
        try {
            fc.position(position);
            fc.write(ByteBuffer.wrap(logBytes));
        } catch (IOException e) {
            Panic.panic(e);
        } finally {
            lock.unlock();
        }

        updateXCheckSum(logBytes);
    }

    private void updateXCheckSum(byte[] logBytes) {
        this.xCheckSum = calcCheck(this.xCheckSum, logBytes);
        try {
            fc.position(0);
            fc.write(ByteBuffer.wrap(Parser.int2byte(this.xCheckSum)));
            fc.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

    @Override
    public void truncate(long position) {
        lock.lock();
        try {
            fc.truncate(position);
        } catch (IOException e) {
            Panic.panic(e);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public byte[] next() {
        lock.lock();
        try {
            byte[] log = internNext();
            if(log == null) return null;
            return Arrays.copyOfRange(log, OF_DATA, log.length);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void rewind() {
        this.position = 4;
    }

    @Override
    public void close() {
        try {
            fc.close();
            raf.close();
        } catch (IOException e) {
            Panic.panic(e);
        }
    }
}
