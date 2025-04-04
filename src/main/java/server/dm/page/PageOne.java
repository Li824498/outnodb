package server.dm.page;

import server.dm.pageCache.PageCache;

import java.util.Arrays;
import java.util.Random;

/**
 * pageOne第一页page，主要用来检测上次数据库是否正常关闭
 * Valid Check
 */
public class PageOne {
    private static final int OF_VC = 100;
    private static final int LEN_VC = 8;


    public static byte[] initRaw() {
        byte[] page = new byte[PageCache.PAGE_SIZE];
        setVcOpenRaw(page);
        return page;
    }

    public static void setVcOpen(Page pg) {
        pg.setDirty(true);
        setVcOpenRaw(pg.getDate());
    }

    private static void setVcOpenRaw(byte[] date) {
        Random random = new Random();
        byte[] bytes = new byte[LEN_VC];
        random.nextBytes(bytes);
        System.arraycopy(bytes, 0, date, OF_VC, LEN_VC);
    }

    public static void setVcClose(Page pg) {
        pg.setDirty(true);
        setVcCloseRaw(pg.getDate());
    }

    private static void setVcCloseRaw(byte[] date) {
        System.arraycopy(date, OF_VC, date, OF_VC + LEN_VC, LEN_VC);
    }

    public static boolean checkVc(Page pg) {
        return Arrays.equals(Arrays.copyOfRange(pg.getDate(), OF_VC, OF_VC + LEN_VC), Arrays.copyOfRange(pg.getDate(), OF_VC + LEN_VC, OF_VC + LEN_VC * 2));
    }
}
