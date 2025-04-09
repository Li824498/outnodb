package server.dm.page;

import server.dm.pageCache.PageCache;
import server.utils.Parser;

import java.util.Arrays;

/**
 *  常规页管理类
 *  管理除了PageOne之外的常规页
 */
public class PageX {

    public static final short OF_FREE = 0;
    public static final short OF_DATE = 2;

    public static byte[] initRaw() {
        byte[] page = new byte[PageCache.PAGE_SIZE];
        setFSO(page, OF_DATE);
        return page;
    }

    public static void setFSO(byte[] page, short fso) {
        System.arraycopy(Parser.short2byte(fso), 0, page, OF_FREE, OF_DATE - OF_FREE);
    }

    public static short getFSO(Page page) {
        return getFSO(page.getData());
    }

    public static short getFSO(byte[] page) {
        return Parser.byte2short(Arrays.copyOfRange(page, OF_FREE, OF_DATE));
    }

    public static int getFreeSpace(Page pg) {
        return PageCache.PAGE_SIZE - (int) getFSO(pg);
    }

    public static short insert(Page page, byte[] raw) {
        page.setDirty(true);
        short offset = getFSO(page);
        System.arraycopy(raw, 0, page, offset, raw.length);
        setFSO(page.getData(), (short) (offset + raw.length));
        return offset;
    }

    public static void recoverInsert(Page page, byte[] raw, short offset) {
        page.setDirty(true);
        System.arraycopy(raw, 0, page.getData(), offset, raw.length);

        if(offset + raw.length > getFSO(page)) {
            setFSO(page.getData(), (short)(offset + raw.length));
        }
    }

    public static void recoverUpdate(Page page, byte[] raw, short offset) {
        page.setDirty(true);
        System.arraycopy(raw, 0, page.getData(), offset, raw.length);
    }

}
