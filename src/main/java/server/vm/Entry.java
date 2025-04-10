package server.vm;

import com.google.common.primitives.Bytes;
import common.SubArray;
import server.dm.dataItem.DataItem;
import server.utils.Panic;
import server.utils.Parser;

import java.util.Arrays;

/**
 * entry类，专门服务vm的包装dataitem，具备对应的底层存储，是byte结构，存储在dataitem的data中，
 * [XMIN][XMAX][data]
 */
public class Entry {

    private static final int LEN_XMIN = 8;
    private static final int LEN_XMAX = 8;

    private long uid;
    private DataItem dm;
    private VersionManager vm;

    public static Entry newEntry(long uid, DataItem dm, VersionManager vm) {
        if(dm == null) return null;

        Entry entry = new Entry();
        entry.uid = uid;
        entry.dm = dm;
        entry.vm = vm;
        return entry;
    }

    public static Entry loadEntry(long uid, VersionManager vm) {
        DataItem dm = ((VersionManagerImpl)vm).dm.read(uid);
        return newEntry(uid, dm, vm);
    }

    public static byte[] wrapEntryRaw(long xid, byte[] data) {
        byte[] xmin = Parser.long2byte(xid);
        byte[] xmax = new byte[LEN_XMAX];
        return Bytes.concat(xmin, xmax, data);
    }

    public void release() {
        ((VersionManagerImpl)vm).releaseEntry(this);
    }

    public void remove() {
        dm.release();
    }

    public byte[] data() {
        dm.rLock();
        try {
            SubArray raw = dm.data();
            return Arrays.copyOfRange(raw.raw, raw.start + LEN_XMIN + LEN_XMAX, raw.end);
        } finally {
            dm.rUnlock();
        }
    }

    public long getXmin() {
        dm.rLock();
        try {
            SubArray raw = dm.data();
            return Parser.byte2long(Arrays.copyOfRange(raw.raw, raw.start, raw.start + LEN_XMIN));
        } finally {
            dm.rUnlock();
        }
    }

    public long getXmax() {
        dm.rLock();
        try {
            SubArray raw = dm.data();
            return Parser.byte2long(Arrays.copyOfRange(raw.raw, raw.start + LEN_XMIN, raw.start + LEN_XMIN + LEN_XMAX));
        } finally {
            dm.rUnlock();
        }
    }

    public void setXmax(long xid) {
        dm.before();
        try {
            SubArray raw = dm.data();
            byte[] xmaxBytes = Parser.long2byte(xid);
            System.arraycopy(xmaxBytes, 0, raw.raw, raw.start, LEN_XMAX);
        } finally {
            dm.after(xid);
        }
    }

    public long getUid() {
        return uid;
    }
}
