package server.vm;

import server.tm.TransactionManagerImpl;

import java.util.HashMap;
import java.util.Map;

public class Transaction {
    public long xid;
    public int level;
    public Map<Long, Boolean> snapshot;
    public Exception err;
    public boolean autoAborted;

    public static Transaction newTransaction(long xid, int level, Map<Long, Transaction> activeTransaction) {
        Transaction t = new Transaction();
        t.xid = xid;
        t.level = level;
        if (level == 1) {
            t.snapshot = new HashMap<>();
            for (Long x : activeTransaction.keySet()) {
                t.snapshot.put(x, true);
            }
        }
        return t;
    }

    public boolean isInSnapshot(long xmax) {
        if (xmax == TransactionManagerImpl.SUPER_ID) {
            return false;
        }
        return snapshot.containsKey(xmax);
    }
}
