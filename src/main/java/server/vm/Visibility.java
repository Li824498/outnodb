package server.vm;

import server.tm.TransactionManager;

public class Visibility {

    /**
     * mysql是怎么解决并发读写同一行数据的？使用for update显式加锁\写操作自动加锁，强制在这几个状态时，令逻辑上的数据与物理上的数据同步，实际上就是禁止发生逻辑物理不一致的行为（在间隙写入数据）
     * 如何解决间隙数据（幻读）？间隙锁
     * outnodb是怎么解决发读写同一行数据的？试用版本跳跃检测
     * 如何解决间隙数据（幻读）？未解决，会有这种问题？ todo 看看问题
     * 禁止用户发生这种行为：逻辑物理不一致，检测到这种行为就报错
     * @param tm
     * @param t
     * @param e
     * @return
     */
    public static boolean isVersionSkip(TransactionManager tm, Transaction t, Entry entry) {
        long xmax = entry.getXmax();
        // 0:读已提交 1:可重复读
        if(t.level == 0) {
            return false;
        } else {
            return tm.isCommitted(xmax) && (xmax > t.xid || t.isInSnapshot(xmax));
        }
        return false;
    }

    public static boolean isVisible(TransactionManager tm, Transaction t, Entry e) {
        if(t.level == 0) {
            return readCommitted(tm, t, e);
        } else {
            return repeatableRead(tm, t, e);
        }
    }

    private static boolean readCommitted(TransactionManager tm, Transaction t, Entry e) {
        long xid = t.xid;
        long xmin = e.getXmin();
        long xmax = e.getXmax();

        if(xmin == xid && xmax == 0) {
            return true;
        }

        if(tm.isCommitted(xmin)) {
            if(xmax == 0) return true;
            if(xmax != xid) {
                if(!tm.isCommitted(xmax)) {
                    return true;
                }
            }
        }
        return false;
    }

    public static boolean repeatableRead(TransactionManager tm, Transaction t, Entry e) {
        long xid = t.xid;
        long xmin = e.getXmin();
        long xmax = e.getXmax();

        if(xmin == xid && xmax != xid) return true;

        if(tm.isCommitted(xmin) && xmin < xid && !t.isInSnapshot(xmin)) {
            if(xmax == 0) return true;
            if(xmax != xid) {
                if(!tm.isCommitted(xid) || xmax > xid || t.isInSnapshot(xmax)) {
                    return true;
                }
            }
        }
        return false;
    }
}
