package server.utils;

import server.tm.TransactionManagerImpl;

import java.nio.ByteBuffer;

import static server.dm.page.PageX.OF_DATE;

public class Parser {

    public static long byte2long(byte[] buf) {
        ByteBuffer buffer = ByteBuffer.wrap(buf, 0, TransactionManagerImpl.XID_HEADER_LENGTH);
        return buffer.getLong();
    }

    public static byte[] long2byte(long xidCounter) {
        return ByteBuffer.allocate(Long.SIZE / Byte.SIZE).putLong(xidCounter).array();
    }

    public static byte[] short2byte(short fso) {
        return ByteBuffer.allocate(Short.SIZE / Byte.SIZE).putShort(fso).array();
    }

    public static short byte2short(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes, 0, OF_DATE);
        return buffer.getShort();
    }

    public static byte[] int2byte(int i) {
        return ByteBuffer.allocate(Integer.SIZE / Byte.SIZE).putInt(i).array();
    }

    public static int byte2int(byte[] array) {
        return ByteBuffer.wrap(array).getInt();
    }
}
