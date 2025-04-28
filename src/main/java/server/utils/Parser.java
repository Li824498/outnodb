package server.utils;

import com.google.common.primitives.Bytes;
import server.tm.TransactionManagerImpl;

import java.nio.ByteBuffer;
import java.util.Arrays;

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

    public static ParseStringRes parseString(byte[] raw) {
        int length = byte2int(Arrays.copyOf(raw, 4));
        String str = new String(Arrays.copyOfRange(raw, 4, 4 + length));
        return new ParseStringRes(str, length + 4);
    }

    public static byte[] string2byte(String str) {
        byte[] l = int2byte(str.length());
        return Bytes.concat(l, str.getBytes());
    }

    public static long str2Uid(String key) {
        long seed = 13331;
        long res = 0;
        for (byte b : key.getBytes()) {
            res = res * seed + (long) b;
        }
        return res;
    }
}
