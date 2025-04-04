package server.dm.page;

import server.dm.pageCache.PageCache;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 通用页存取类
 * 只提供所有页存取的公共逻辑部分，需配合pageX和pageOne完成完整的存取操作
 *
 */

public class PageImpl implements Page{

    private int pageNumber;
    private boolean dirty;
    private byte[] date;
    private Lock lock;

    private PageCache pc;

    public PageImpl(int pageNumber, byte[] date, PageCache pc) {
        this.pageNumber = pageNumber;
        this.date = date;
        this.lock = new ReentrantLock();
        this.pc = pc;
    }

    @Override
    public void lock() {
        lock.lock();
    }

    @Override
    public void unlock() {
        lock.unlock();
    }

    @Override
    public void release() {
        pc.release(this);
    }

    @Override
    public void setDirty(boolean dirty) {
        this.dirty = dirty;
    }

    @Override
    public boolean isDirty() {
        return dirty;
    }

    @Override
    public int getPageNumber() {
        return pageNumber;
    }

    @Override
    public byte[] getDate() {
        return date;
    }
}
