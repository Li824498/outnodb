package server.dm.pageIndex;

import server.dm.page.Page;

public class PageInfo {
    Page page;
    int freeSpace;

    PageInfo(Page page, int freeSpace) {
        this.page = page;
        this.freeSpace = freeSpace;
    }
}
