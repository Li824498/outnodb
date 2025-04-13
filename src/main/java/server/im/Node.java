package server.im;

import common.SubArray;
import server.dm.dataItem.DataItem;
import server.dm.dataItem.DataItemImpl;
import server.utils.Parser;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * 结构存储:
 * [LeafFlag][KeyNumber][SiblingUid]
 * 0 -------1----------3-----------11
 * [Son0][Key0][Son1][Key1]...[SonN][KeyN]
 */
public class Node {

    private static final int LEAF_OFFSET = 0;
    private static final int KEY_NUMBER_OFFSET = LEAF_OFFSET + 1;
    private static final int SIBLING_OFFSET = KEY_NUMBER_OFFSET + 2;
    private static final int NODE_HEADER_SIZE = 11;


    private static final int BALANCE_NUMBER = 32;
    private static final int NODE_SIZE = NODE_HEADER_SIZE + (2 * 8) * (BALANCE_NUMBER * 2 + 2);

    BPlusTree tree;
    DataItem dataItem;
    SubArray raw;
    long uid;

    static void setRawIsLeaf(SubArray raw, boolean isLeaf) {
        raw.raw[raw.start + LEAF_OFFSET] = isLeaf ? (byte) 0 : (byte) 1;
    }

    static boolean getRawIsLeaf(SubArray raw) {
        return raw.raw[raw.start + LEAF_OFFSET] == (byte) 1;
    }

    static void setRawNoKeys(SubArray raw, int noKeys) {
        System.arraycopy(Parser.short2byte((short) noKeys), 0, raw.raw, raw.start + KEY_NUMBER_OFFSET, 2);
    }

    static int getRawNoKeys(SubArray raw) {
        return (int) Parser.byte2short(Arrays.copyOfRange(raw.raw, raw.start + KEY_NUMBER_OFFSET, raw.start + KEY_NUMBER_OFFSET + 2));
    }

    static void setRawSibling(SubArray raw, long sibling) {
        System.arraycopy(Parser.long2byte(sibling), 0, raw.raw, raw.start + SIBLING_OFFSET, 8);
    }

    static long getRawSibling(SubArray raw) {
        return Parser.byte2long(Arrays.copyOfRange(raw.raw, raw.start + SIBLING_OFFSET, raw.start + SIBLING_OFFSET + 8));
    }

    static void setRawKthSon(SubArray raw, long uid, int kth) {
        int offset = raw.start + NODE_HEADER_SIZE + kth * (2 * 8);
        System.arraycopy(Parser.long2byte(uid), 0, raw.raw, offset, 8);
    }

    static long getRawKthSon(SubArray raw, int kth) {
        int offset = raw.start + NODE_HEADER_SIZE + kth * (2 * 8);
        return Parser.byte2long(Arrays.copyOfRange(raw.raw, offset, offset + 8));
    }

    static void setRawKthKey(SubArray raw, long key, int kth) {
        int offset = raw.start + NODE_HEADER_SIZE + kth * (2 * 8) + 8;
        System.arraycopy(Parser.long2byte(key), 0, raw.raw, offset, 8);
    }

    static long getRawKthKey(SubArray raw, int kth) {
        int offset = raw.start + NODE_HEADER_SIZE + kth * (2 * 8) + 8;
        return Parser.byte2long(Arrays.copyOfRange(raw.raw, offset, offset + 8));
    }

    // todo 干啥用的
    static byte[] newRootRaw(long left, long right, long key) {
        SubArray raw = new SubArray(new byte[NODE_SIZE], 0, NODE_SIZE);

        setRawIsLeaf(raw, false);
        setRawNoKeys(raw, 0);
        setRawSibling(raw, 0);
        setRawKthSon(raw, left, 0);
        setRawKthKey(raw, key, 0);
        setRawKthSon(raw, right, 1);
        setRawKthKey(raw, Long.MAX_VALUE, 1);

        return raw.raw;
    }

    static byte[] newNilRootRaw() {
        SubArray raw = new SubArray(new byte[NODE_SIZE], 0, NODE_SIZE);

        setRawNoKeys(raw, 0);
        setRawSibling(raw, 0);

        return raw.raw;
    }

    static Node loadNode(BPlusTree bTree, long uid) {
        DataItem di = bTree.dm.read(uid);
        Node n = new Node();
        n.tree = bTree;
        n.dataItem = di;
        n.raw = di.data();
        n.uid = uid;
        return n;
    }

    public void release() {
        dataItem.release();
    }

    public boolean isLeaf() {
        dataItem.rLock();
        try {
            return getRawIsLeaf(raw);
        } finally {
            dataItem.rUnlock();
        }
    }

    class SearchNextRes {
        long uid;
        long siblingUid;
    }

    public SearchNextRes searchNext(long key) {
        dataItem.rLock();
        try {
            SearchNextRes res = new SearchNextRes();
            int noKeys = getRawNoKeys(raw);
            for (int i = 0; i < noKeys; i++) {
                long iKey = getRawKthKey(raw, i);
                if (key < iKey) {
                    res.uid = getRawKthSon(raw, i);
                    res.siblingUid = 0;
                    return res;
                }
            }
            res.uid = 0;
            res.siblingUid = getRawSibling(raw);
            return res;

        } finally {
            dataItem.rUnlock();
        }
    }

    class LeafSearchRangeRes {
        List<Long> uids;
        long siblingUid;
    }

    public LeafSearchRangeRes leafSearchRangeRes(long leftKey, long rightKey) {
        dataItem.rLock();
        try {
            int noKeys = getRawNoKeys(raw);
            int kth = 0;
            while (kth < noKeys) {
                long iKey = getRawKthKey(raw, kth);
                if(iKey >= leftKey) {
                    break;
                }
                kth++;
            }
            List<Long> uids = new ArrayList<>();
            while (kth < noKeys) {
                long iKey = getRawKthKey(raw, kth);
                if (iKey <= rightKey) {
                    uids.add(getRawKthSon(raw, kth));
                    kth++;
                } else {
                    break;
                }
            }
            long siblingUid = 0;
            if (kth == noKeys) {
                siblingUid = getRawSibling(raw);
            }
            LeafSearchRangeRes res = new LeafSearchRangeRes();
            res.uids = uids;
            res.siblingUid = siblingUid;
            return res;
        } finally {
            dataItem.rUnlock();
        }
    }
}
