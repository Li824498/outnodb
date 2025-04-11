package server.vm;

import server.dm.DataManager;
import server.tm.TransactionManager;

public interface VersionManager {
    byte[] read(long xid, long uid);
    long insert(long xid, byte[] data);
    boolean delete(long xid, long uid);

    long begin(int level);
    void commit(long xid);
    void abort(long xid);

    public static VersionManager newVersionManager(TransactionManager tm, DataManager dm) {
        return new VersionManagerImpl(tm, dm);
    }
}
