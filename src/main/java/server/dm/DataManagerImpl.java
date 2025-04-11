package server.dm;

import common.Error;
import server.common.AbstractCache;
import server.dm.dataItem.DataItem;
import server.dm.dataItem.DataItemImpl;
import server.dm.logger.Logger;
import server.dm.page.Page;
import server.dm.page.PageOne;
import server.dm.page.PageX;
import server.dm.pageCache.PageCache;
import server.dm.pageIndex.PageIndex;
import server.dm.pageIndex.PageInfo;
import server.tm.TransactionManager;
import server.utils.Panic;
import server.utils.Types;

public class DataManagerImpl extends AbstractCache<DataItem> implements DataManager{
    public PageCache pc;
    // todo tm是不是可以删掉？
    public TransactionManager tm;
    public Logger lg;
    public PageIndex pIndex;
    public Page pageOne;

    public DataManagerImpl(PageCache pc, TransactionManager tm, Logger lg) {
        super(0);
        this.pc = pc;
        this.tm = tm;
        this.lg = lg;
        this.pIndex = new PageIndex();
    }


    @Override
    public DataItem read(long uid) {
        DataItemImpl dataItem = (DataItemImpl) super.get(uid);
        if(!dataItem.isValid()) {
            // 失效了，那就没必要占用资源了
            dataItem.release();
            return null;
        }
        return dataItem;
    }

    @Override
    public long insert(long xid, byte[] data) {
        byte[] raw = DataItem.wrapDataItemRaw(data);
        if(raw.length >= PageX.MAX_FREE_SPACE) {
            Panic.panic(Error.DataTooLargeException());
        }

        PageInfo pi = null;
        for (int i = 0; i < 5; i++) {
            pi = pIndex.select(raw.length);
            if (pi != null) {
                break;
            } else {
                int newPgno = pc.newPage(PageX.initRaw());
                pIndex.add(newPgno, PageX.MAX_FREE_SPACE);
            }
        }
        if(pi == null) {
            Panic.panic(Error.DataBaseBusyException());
        }

        Page pg = null;
        try {
            pg = pc.getPage(pi.pgno);
            byte[] log = Recover.insertLog(xid, pg, raw);
            lg.log(log);

            short offset = PageX.insert(pg, raw);

            pg.release();
            return Types.addressToUid(pi.pgno, offset);
        } finally {
            if(pg == null) {
                pIndex.add(pi.pgno, 0);
            } else {
                pIndex.add(pi.pgno, PageX.getFreeSpace(pg));
            }
        }
    }

    @Override
    public void close() {
        super.close();
        lg.close();

        PageOne.setVcClose(pageOne);
        pageOne.release();
        pc.close();
    }

    public void logDataItem(Long xid, DataItemImpl di) {
        byte[] log = Recover.updateLog(xid, di);
        lg.log(log);
    }

    public void releaseDataItem(DataItemImpl dataItem) {
        super.release(dataItem.getUid());
    }

    @Override
    protected DataItem getForCache(long uid) throws Exception {
        short pgno = (short) (uid & (1 << 16 - 1));
        uid >>>= 32;
        int offset = (int) uid;
        Page pg = pc.getPage(pgno);
        return DataItem.parseDataItem(pg, offset, this);
    }

    @Override
    protected void releaseForCache(DataItem di) {
        di.page().release();
    }

    public void initPageOne() {
        int pgno = pc.newPage(PageOne.initRaw());
        assert pgno == 1;
        Page page = null;
        try {
            page = pc.getPage(pgno);
        } catch (Exception e) {
            Panic.panic(e);
        }
        pc.flushPage(page);
    }

    public boolean loadCheckPageOne() {
        Page pageOne = null;
        try {
            pageOne = pc.getPage(1);
        } catch (Exception e) {
            Panic.panic(e);
        }
        return PageOne.checkVc(pageOne);
    }

    public void fillPageIndex() {
        int pageNumber = pc.getPageNumber();
        for (int i = 2; i < pageNumber; i++) {
            Page page = null;
            try {
                page = pc.getPage(pageNumber);
            } catch (Exception e) {
                Panic.panic(e);
            }
            pIndex.add(page.getPageNumber(), PageX.getFreeSpace(page));

            page.release();
        }
    }
}
