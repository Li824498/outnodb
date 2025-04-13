package server.vm;

import common.Error;
import server.utils.Panic;

import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class LockTable {
    private Map<Long, List<Long>> x2u;
    private Map<Long, Long> u2x;
    private Map<Long, Long> waitting;
    private Map<Long, List<Long>> waitList;
    private Map<Long, Lock> waitLock;
    private Lock lock;

    public LockTable() {
        this.x2u = new HashMap<>();
        this.u2x = new HashMap<>();
        this.waitting = new HashMap<>();
        this.waitList = new HashMap<>();
        this.waitLock = new HashMap<>();
        this.lock = new ReentrantLock();
    }

    public Lock add(long xid, long uid) throws Exception {
        lock.lock();
        try {
            if (isInList(x2u, xid, uid)) {
                return null;
            }
            if (!u2x.containsKey(uid)) {
                u2x.put(uid, xid);
                putIntoList(x2u, xid, uid);
                return null;
            }

            waitting.put(xid, uid);
            putIntoList(waitList, uid, xid);
            if (hasDeadLock()) {
                waitting.remove(xid);
                removeFromList(waitList, xid, uid);
                throw Error.DeadLockException();
            }

            Lock l = new ReentrantLock();
            l.lock();
            waitLock.put(xid, l);
            return l;

        } finally {
            lock.unlock();
        }
    }

    // for commit
    public void remove(long xid) {
        lock.lock();
        try {
            List<Long> uids = x2u.get(xid);
            if (uids != null) {
                while (!uids.isEmpty()) {
                    Long uid = uids.remove(0);
                    wakeUpXid(uid);
                }
            }
            waitting.remove(xid);
            x2u.remove(xid);
            waitLock.remove(xid);
        } finally {
            lock.unlock();
        }

    }

    private void wakeUpXid(Long uid) {
/*        List<Long> xids = waitList.get(uid);
        Long xid = xids.remove(0);
        Lock l = waitLock.remove(xid);
        l.unlock();*/

        u2x.remove(uid);
        List<Long> xids = waitList.get(uid);
        if (xids == null) {
            return;// 无可释放
        }
        assert xids.size() > 0;

        while (xids.size() > 0) {
            Long xid = xids.remove(0);
            if (!waitLock.containsKey(xid)) {
                continue;
            } else {
                u2x.put(uid, xid);
                waitting.remove(xid);
                Lock l = waitLock.get(xid);
                l.unlock();
                break;
            }
        }

        if (xids.size() == 0) waitList.remove(uid);
    }

    Map<Long, Integer> xidStamp;
    Integer stamp;

    private boolean hasDeadLock() {
        xidStamp = new HashMap<>();
        stamp = 1;
        for (Long xid : waitting.keySet()) {
            if (xidStamp.containsKey(xid)) {
                continue;
            }
            if (dfs(xid)) {
                return true;
            }
            stamp++;
        }
        return false;
    }

    private boolean dfs(Long xid) {
        Integer s = xidStamp.get(xid);
        if (s != null && s.equals(stamp)) {
            return true;
        }
        if (s != null && s < stamp) {
            return false;
        }
        xidStamp.put(xid, stamp);

        Long uid = waitting.get(xid);
        if (uid == null) return false;
        Long nextXid = u2x.get(uid);
        assert nextXid != null;
        return dfs(nextXid);
    }

    private void removeFromList(Map<Long, List<Long>> waitList, long uid, long xid) {
/*        lock.lock();
        try {
            waitList.get(xid).remove(uid)
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            lock.unlock();
        }*/
        List<Long> l = waitList.get(uid);
        if (l == null) return;
        Iterator<Long> i = l.iterator();
        while (i.hasNext()) {
            long e = i.next();
            if (e == xid) {
                i.remove();
                break;
            }
        }
        if (l.size() == 0) {
            waitList.remove(uid);
        }
    }

    private void putIntoList(Map<Long, List<Long>> x2u, long xid, long uid) {
        if (!x2u.containsKey(xid)) {
            x2u.put(xid, new ArrayList<>());
        }
        x2u.get(xid).add(0, uid);

    }

    private boolean isInList(Map<Long, List<Long>> x2u, long xid, long uid) {
        List<Long> uids = x2u.get(xid);
        if (uids == null) {
            return false;
        }
        Iterator<Long> i = uids.iterator();
        while (i.hasNext()) {
            long e = i.next();
            if (e == uid) {
                return true;
            }
        }
        return false;

    }
}
