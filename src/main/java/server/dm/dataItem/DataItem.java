package server.dm.dataItem;

import com.google.common.primitives.Bytes;
import common.SubArray;
import server.dm.DataManagerImpl;
import server.dm.page.Page;
import server.utils.Parser;
import server.utils.Types;

import java.util.Arrays;

public interface DataItem {
    SubArray data();

    void before();
    void unBefore();
    void after(Long xid);
    void release();

    void lock();
    void unlock();
    void rLock();
    void rUnlock();

    Page page();
    long getUid();
    byte[] getOldRaw();
    SubArray getRaw();

    public static byte[] wrapDataItemRaw(byte[] raw) {
        byte[] valid = new byte[1];
        byte[] size = Parser.int2byte(raw.length);
        return Bytes.concat(valid, size, raw);
    }

    public static DataItem parseDataItem(Page pg, int offset, DataManagerImpl dm) {
        byte[] data = pg.getData();
        short dataSize = Parser.byte2short(Arrays.copyOfRange(data, offset + DataItemImpl.OF_SIZE, offset + DataItemImpl.OF_SIZE + DataItemImpl.LEN_SIZE));
        short length = (short) (dataSize + DataItemImpl.LEN_VALID + DataItemImpl.LEN_SIZE);
        long uid = Types.addressToUid(pg.getPageNumber(), offset);
        return new DataItemImpl(new SubArray(data, offset, offset + length), new byte[length], pg, uid, dm);
    }

    public static void setDataItemRawInvalid(byte[] raw) {
        raw[DataItemImpl.OF_VALID] = (byte) 1;
    }
}
