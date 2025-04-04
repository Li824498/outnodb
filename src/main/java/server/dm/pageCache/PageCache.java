package server.dm.pageCache;

import server.dm.page.Page;

public interface PageCache {
    void release(Page page);
}
