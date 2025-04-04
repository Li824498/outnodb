package server.dm.pageCache;

import common.Error;
import server.dm.page.Page;
import server.utils.Panic;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

public interface PageCache {
    final static int PAGE_SIZE = 1 << 13;

    int newPage(byte[] initData);
    Page getPage(int pgno);
    void release(Page page);

    void close();

    void truncateyByPgno(int maxPgno);
    int getPageNumber();
    void flushPage(Page page);

    public static PageCacheImpl create(String path, long memory) {
        File file = new File(path + PageCacheImpl.DB_SUFFIX);
        try {
            if(!file.createNewFile()){
                Panic.panic(Error.FileExistsException());
            }
        } catch (IOException e) {
            Panic.panic(e);
        }

        RandomAccessFile raf = null;
        FileChannel fc = null;

        try {
            raf = new RandomAccessFile(file, "rw");
            fc = raf.getChannel();
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }

        return new PageCacheImpl(raf, fc, (int) (memory/PAGE_SIZE));
    }

    public static PageCacheImpl open(String path, long memory) {
        File file = new File(path + PageCacheImpl.DB_SUFFIX);
        if(!file.exists()) {
            Panic.panic(Error.FileNotExistsException());
        }
        RandomAccessFile raf = null;
        FileChannel fc = null;
        try {
            raf = new RandomAccessFile(file, "rw");
            fc = raf.getChannel();
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }
        return new PageCacheImpl(raf, fc, (int) (memory/PAGE_SIZE));
    }
}
