package server.dm.pageIndex;

import server.dm.page.Page;
import server.dm.pageCache.PageCache;
import server.utils.Panic;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class PageIndex {
    private static final int INTERVALS_NO = 40;
    private static final int THRESHOLD = PageCache.PAGE_SIZE / INTERVALS_NO;

    private List<PageInfo>[] lists;
    private Lock lock;

    public PageIndex() {
        lock = new ReentrantLock();
        lists = new List[INTERVALS_NO + 1];
        for(List list : lists) {
            list = new ArrayList();
        }
    }

    public void add(Page page, int freeSpace) {
        int number = freeSpace / THRESHOLD;
        lock.lock();
        try {
            lists[number].add(new PageInfo(page, freeSpace));
        } catch (Exception e) {
            Panic.panic(e);
        } finally {
            lock.unlock();
        }
    }

    public PageInfo select(int freeSpace) {
        int number = freeSpace / THRESHOLD;
        lock.lock();
        try {
            if (number < INTERVALS_NO) number++;
            while(number <= INTERVALS_NO) {
                if(lists[number].isEmpty()) {
                    number++;
                    continue;
                }

                return lists[number].remove(0);
            }
            return null;
        } finally {
            lock.unlock();
        }
    }
}
