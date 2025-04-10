package server.dm;

import server.dm.dataItem.DataItem;
import server.dm.dataItem.DataItemImpl;
import server.dm.logger.Logger;
import server.dm.logger.LoggerImpl;
import server.dm.page.PageOne;
import server.dm.pageCache.PageCache;
import server.dm.pageCache.PageCacheImpl;
import server.tm.TransactionManager;

public interface DataManager {
    DataItem read(long uid);
    long insert(long xid, byte[] data);
    void close();

    public static DataManager create(String path, long mem, TransactionManager tm) {
        PageCacheImpl pc = PageCache.create(path, mem);
        LoggerImpl lg = Logger.create(path);

        DataManagerImpl dm = new DataManagerImpl(pc, tm, lg);
        dm.initPageOne();
        return dm;
    }

    public static DataManager open(String path, long mem, TransactionManager tm) {
        PageCacheImpl pc = PageCache.open(path, mem);
        LoggerImpl lg = Logger.open(path);
        DataManagerImpl dm = new DataManagerImpl(pc, tm, lg);
        if(!dm.loadCheckPageOne()) {
            Recover.recover(tm, lg, pc);
        }
        dm.fillPageIndex();
        PageOne.setVcOpen(dm.pageOne);
        dm.pc.flushPage(dm.pageOne);

        return dm;
    }
}
