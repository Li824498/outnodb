package server.dm.pageIndex;

import server.dm.page.Page;

public class PageInfo {
    public int pgno;
    public int freeSpace;

    PageInfo(int pgno, int freeSpace) {
        this.pgno = pgno;
        this.freeSpace = freeSpace;
    }
}
