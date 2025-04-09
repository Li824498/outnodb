package server.dm.pageIndex;

import server.dm.page.Page;
import server.dm.pageCache.PageCache;
import server.utils.Panic;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 1. 互斥条件：每个锁（locks[number] 和 lockAll）只能由一个线程持有，其他线程不能同时访问。
 * 2. 占有且等待：线程可能在持有一个锁时等待其他锁。
 * 3. 不剥夺条件：锁不能被其他线程强行夺走，必须由持有线程释放。
 * 4. 循环等待条件：线程没有形成相互等待的循环，因此不会发生死锁。
 */

public class PageIndex {
    private static final int INTERVALS_NO = 40;
    private static final int THRESHOLD = PageCache.PAGE_SIZE / INTERVALS_NO;

    private List<PageInfo>[] lists;
    private Lock[] locks;
    private Lock lockAll;

    public PageIndex() {
        lists = new List[INTERVALS_NO + 1];
        for(List list : lists) {
            list = new ArrayList();
        }
        locks = new ReentrantLock[INTERVALS_NO + 1];
        lockAll = new ReentrantLock();
    }

    public void add(Page page, int freeSpace) {
        int number = freeSpace / THRESHOLD;
        locks[number].lock();
        try {
            lockAll.lock();
            try {
                lists[number].add(new PageInfo(page, freeSpace));
            } catch (Exception e) {
                Panic.panic(e);
            } finally {
                lockAll.unlock();
            }
        } catch (Exception e) {
            Panic.panic(e);
        } finally {
            locks[number].unlock();
        }
    }

    public PageInfo select(int freeSpace) {
        int number = freeSpace / THRESHOLD;
        lockAll.lock();
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
            lockAll.unlock();
        }
    }
}
