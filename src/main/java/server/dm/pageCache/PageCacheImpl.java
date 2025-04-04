package server.dm.pageCache;

import server.common.AbstractCache;
import server.dm.page.Page;

public class PageCacheImpl extends AbstractCache<Page> implements PageCache{
    public PageCacheImpl(int maxResource) {
        super(maxResource);
    }

    @Override
    protected Page getForCache(long key) throws Exception {
        return null;
    }

    @Override
    protected void releaseForCache(Page obj) {

    }
}
