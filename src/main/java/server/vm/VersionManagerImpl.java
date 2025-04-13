package server.vm;

import common.Error;
import server.common.AbstractCache;
import server.dm.DataManager;
import server.tm.TransactionManager;
import server.utils.Panic;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class VersionManagerImpl extends AbstractCache<Entry> implements VersionManager{

    private TransactionManager tm;
    public DataManager dm;
    private Lock lock;
    // todo 是不是能优化成chm
    private Map<Long, Transaction> activeTransaction;
    LockTable lt;

    public VersionManagerImpl(TransactionManager tm, DataManager dm) {
        super(0);
        this.tm = tm;
        this.dm = dm;
        this.lock = new ReentrantLock();
        this.activeTransaction = new HashMap<Long, Transaction>();
        this.lt = new LockTable();
    }


    @Override
    public byte[] read(long xid, long uid) throws Exception {
        // todo 为什么要加锁？
        lock.lock();
        Transaction t = activeTransaction.get(xid);
        lock.unlock();

        if(t.err != null) {
            throw t.err;
        }

        Entry entry = null;
        try {
            entry = super.get(uid);
        } catch (Exception e) {
            if(e == Error.NullEntryException) {
                return null;
            } else {
                throw e;
            }
        }
        try {
            if (Visibility.isVisible(tm, t, entry)) {
                return entry.data();
            } else {
                return null;
            }
        } finally {
            entry.release();
        }
    }

    @Override
    public long insert(long xid, byte[] data) throws Exception {
        lock.lock();
        Transaction t = activeTransaction.get(xid);
        lock.unlock();

        if(t.err != null) {
            throw t.err;
        }

        byte[] raw = Entry.wrapEntryRaw(xid, data);
        return dm.insert(xid, raw);

    }

    @Override
    public boolean delete(long xid, long uid) throws Exception {
        lock.lock();
        Transaction t = activeTransaction.get(xid);
        lock.unlock();

        if(t.err != null) {
            throw t.err;
        }
        Entry entry = null;
        try {
            entry = super.get(uid);
        } catch (Exception e) {
            if (e == Error.NullEntryException) {
                return false;
            } else {
                throw e;
            }
        }

        try {
            if(!Visibility.isVisible(tm, t, entry)) {
                return false;
            }
            Lock l = null;
            try {
                l = lt.add(xid, uid);
            } catch (Exception e) {
                // todo
                t.err = Error.ConcurrentUpdateException;
                internAbort(xid, true);
                t.autoAborted = true;
                throw t.err;
            }
            if(l != null) {
                l.lock();
                l.unlock();
            }

            if(entry.getXmax() == xid) {
                return false;
            }

            // todo
            if(Visibility.isVersionSkip(tm, t, entry)) {
                t.err = Error.ConcurrentUpdateException;
                internAbort(xid, true);
                t.autoAborted = true;
                throw t.err;
            }

            entry.setXmax(xid);
            return true;

        } finally {
            entry.release();
        }
    }

    private void internAbort(long xid, boolean autoAborted) {
        lock.lock();
        Transaction transaction = activeTransaction.get(xid);
        if(!autoAborted) {
            activeTransaction.remove(xid);
        }
        lock.unlock();

        if(transaction.autoAborted) return;
        lt.remove(xid);
        tm.abort(xid);
    }

    @Override
    public long begin(int level) {
        lock.lock();
        try {
            long xid = tm.begin();
            Transaction t = Transaction.newTransaction(xid, level, activeTransaction);
            activeTransaction.put(xid, t);
            return xid;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void commit(long xid) {
        lock.lock();
        Transaction t = activeTransaction.get(xid);
        lock.unlock();

        try {
            if(t.err != null) {
                throw t.err;
            }
        } catch (Exception e) {
            Panic.panic(e);
        }

        lock.lock();
        activeTransaction.remove(xid);
        lock.unlock();

        lt.remove(xid);
        tm.commit(xid);
    }

    @Override
    public void abort(long xid) {
        internAbort(xid, false);
    }

    public void releaseEntry(Entry entry) {
        super.release(entry.getUid());
    }

    @Override
    protected Entry getForCache(long uid) throws Exception {
        Entry entry = Entry.loadEntry(uid, this);
        if(entry == null) {
            throw Error.NullEntryException;
        }
        return entry;
    }

    @Override
    protected void releaseForCache(Entry entry) {
        entry.remove();
    }
}
