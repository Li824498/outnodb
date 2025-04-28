package server.im;

import common.SubArray;
import server.dm.DataManager;
import server.dm.dataItem.DataItem;
import server.dm.dataItem.DataItemImpl;
import server.tm.TransactionManagerImpl;
import server.utils.Parser;
import server.im.Node.SearchNextRes;
import server.im.Node.LeafSearchRangeRes;
import server.im.Node.InsertAndSplitRes;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class BPlusTree {
    public DataManager dm;
    private long bootUid;
    private DataItem bootDataItem;
    private Lock bootLock;

    public static long create(DataManager dm) {
        byte[] rawRoot = Node.newNilRootRaw();
        long rootUid = dm.insert(TransactionManagerImpl.SUPER_ID, rawRoot);
        return dm.insert(TransactionManagerImpl.SUPER_ID, Parser.long2byte(rootUid));
    }

    public static BPlusTree load(long bootUid, DataManager dm) {
        DataItem bootDataItem = dm.read(bootUid);
        BPlusTree t = new BPlusTree();
        t.bootUid = bootUid;
        t.dm = dm;
        t.bootDataItem = bootDataItem;
        t.bootLock = new ReentrantLock();
        return t;
    }

    private long rootUid() {
        bootLock.lock();
        try {
            SubArray sa = bootDataItem.data();
            return Parser.byte2long(Arrays.copyOfRange(sa.raw, sa.start, sa.start + 8));
        } finally {
            bootLock.unlock();
        }
    }

    private void updateRootUid(long left, long right, long rightKey) {
        bootLock.lock();
        try {
            byte[] rootRaw = Node.newRootRaw(left, right, rightKey);
            long newBootUid = dm.insert(TransactionManagerImpl.SUPER_ID, rootRaw);
            bootDataItem.before();
            SubArray diRaw = bootDataItem.data();
            System.arraycopy(Parser.long2byte(newBootUid), 0, diRaw.raw, diRaw.start, 8);
            bootDataItem.after(TransactionManagerImpl.SUPER_ID);
        } finally {
            bootLock.unlock();
        }
    }

    private long searchLeaf(long nodeUid, long key) {
        Node node = Node.loadNode(this, nodeUid);
        boolean isLeaf = node.isLeaf();
        node.release();

        if (isLeaf) {
            return nodeUid;
        } else {
            long next = searchNext(nodeUid, key);
            return searchLeaf(next, key);
        }
    }

    private long searchNext(long nodeUid, long key) {
        while (true) {
            Node node = Node.loadNode(this, nodeUid);
            SearchNextRes res = node.searchNext(key);
            node.release();
            if (res.uid != 0) {
                return res.uid;
            }
            nodeUid = res.siblingUid;
        }
    }

    public List<Long> search(long key) {
        return searchRange(key, key);
    }

    public List<Long> searchRange(long leftKey, long rightKey) {
        long rootUid = rootUid();
        long leafUid = searchLeaf(rootUid, leftKey);
        List<Long> uids = new ArrayList<>();
        while (true) {
            Node leaf = Node.loadNode(this, leafUid);
            LeafSearchRangeRes res = leaf.leafSearchRangeRes(leftKey, rightKey);
            leaf.release();
            uids.addAll(res.uids);
            if(res.siblingUid == 0) {
                break;
            } else {
                leafUid = res.siblingUid;
            }
        }
        return uids;
    }

    class InsertRes{
        long newNode;
        long newKey;
    }

    public void insert(long key, long uid) throws Exception {
        long rootUid = rootUid();
        InsertRes res = insert(rootUid, uid, key);
        if (res.newNode != 0) {
            updateRootUid(rootUid, res.newNode, res.newKey);
        }
    }

    private InsertRes insert(long nodeUid, long uid, long key) throws Exception {
        Node node = Node.loadNode(this, nodeUid);
        boolean isLeaf = node.isLeaf();
        node.release();

        InsertRes res = null;
        if (isLeaf) {
            res = insertAndSplit(nodeUid, uid, key);
        } else {
            long next = searchLeaf(nodeUid, key);
            InsertRes ires = insert(next, uid, key);
            if (ires.newNode != 0) {
                res = insertAndSplit(nodeUid, ires.newNode, ires.newKey);
            } else {
                res = new InsertRes();
            }
        }
        return res;
    }

    private InsertRes insertAndSplit(long nodeUid, long uid, long key) throws Exception {
        while (true) {
            Node node = Node.loadNode(this, nodeUid);
            InsertAndSplitRes iasr = node.insertAndSplit(uid, key);
            node.release();
            if (iasr.siblingUid != 0) {
                nodeUid = iasr.siblingUid;
            } else {
                InsertRes res = new InsertRes();
                res.newKey = iasr.newKey;
                res.newNode = iasr.newSon;
                return res;
            }
        }
    }

    public void close() {
        bootDataItem.release();
    }
}
