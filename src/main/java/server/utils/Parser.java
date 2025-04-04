package server.utils;

import server.tm.TransactionManagerImpl;

import java.nio.ByteBuffer;

public class Parser {

    public static long byte2long(byte[] buf) {
        ByteBuffer buffer = ByteBuffer.wrap(buf, 0, TransactionManagerImpl.XID_HEADER_LENGTH);
        return buffer.getLong();
    }

    public static byte[] long2byte(long xidCounter) {
        return ByteBuffer.allocate(Long.SIZE / Byte.SIZE).putLong(xidCounter).array();
    }
}
