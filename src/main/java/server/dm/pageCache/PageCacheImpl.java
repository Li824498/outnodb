package server.dm.pageCache;

import server.common.AbstractCache;
import server.dm.page.Page;
import server.dm.page.PageImpl;
import server.utils.Panic;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 页缓存类，负责页、页缓存的管理
 */
public class PageCacheImpl extends AbstractCache<Page> implements PageCache{
    public static final String DB_SUFFIX = ".db";

    private RandomAccessFile raf;
    private FileChannel fc;
    private Lock lock;

    private AtomicInteger pageNumbers;

    public PageCacheImpl(RandomAccessFile raf, FileChannel fc, int maxResource) {
        super(maxResource);
        this.raf = raf;
        this.fc = fc;
        this.lock = new ReentrantLock();
        long length = 0;
        try {
            length = raf.length();
        } catch (IOException e) {
            Panic.panic(e);
        }

        this.pageNumbers = new AtomicInteger((int) (length / PAGE_SIZE));
    }

    @Override
    protected Page getForCache(long key) throws Exception {
        int pgno = (int) key;
        long pageOffset = pageOffset(pgno);
        lock.lock();
        ByteBuffer buf = ByteBuffer.allocate(PAGE_SIZE);
        try {
            fc.position(pageOffset);
            fc.read(buf);
        } catch (IOException e) {
            Panic.panic(e);
        } finally {
            lock.unlock();
        }
        return new PageImpl(pgno, buf.array(), this);
    }

    @Override
    protected void releaseForCache(Page page) {
        if(page.isDirty()) {
            flush(page);
            page.setDirty(false);
        }
    }

    @Override
    public int newPage(byte[] initData) {
        int pgno = pageNumbers.incrementAndGet();
        PageImpl page = new PageImpl(pgno, initData, null);
        flush(page);
        return pgno;
    }

    public void flush(Page page) {
        int pgno = page.getPageNumber();
        long offset = pageOffset(pgno);

        lock.lock();
        try {
            fc.position(offset);
            fc.write(ByteBuffer.wrap(page.getData()));
            fc.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        } finally {
            lock.unlock();
        }
    }

    private long pageOffset(int pgno) {
        return (long) (pgno - 1) * PAGE_SIZE;
    }

    @Override
    public Page getPage(int pgno) {
        return super.get(pgno);
    }

    @Override
    public void release(Page page) {
        super.release(page.getPageNumber());
    }

    @Override
    public void close() {
        try {
            raf.close();
            fc.close();
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

    @Override
    public void truncateyByPgno(int maxPgno) {
        long maxSize = pageOffset(maxPgno + 1);
        try {
            raf.setLength(maxSize);
        } catch (IOException e) {
            Panic.panic(e);
        }
        pageNumbers.set(maxPgno);
    }

    @Override
    public int getPageNumber() {
        return pageNumbers.intValue();
    }

    @Override
    public void flushPage(Page page) {
        flush(page);
    }
}
