package server.dm.pageCache;

import server.dm.page.Page;

public interface PageCache {
    final static int PAGE_SIZE = 1 << 13;

    void release(Page page);
}
