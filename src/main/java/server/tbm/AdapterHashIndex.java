package server.tbm;

import server.common.AbstractCache;

import java.util.List;

public class AdapterHashIndex extends AbstractCache<List<Long>> {
    public AdapterHashIndex() {
        super(1000);
    }

    @Override
    protected List<Long> getForCache(long key) throws Exception {
        return null;
    }

    @Override
    protected void releaseForCache(List<Long> obj) {

    }

    public synchronized List<Long> getHashCache(long key) {
        Node node;
        if (newCache.containsKey(key)) {
            node = newCache.get(key);
            moveToHead(node, false);
        } else {
            node = oldCache.get(key);
            moveToHead(node, true);
        }

        return node.getContent();
    }

    public synchronized void addHashCache(long key, List<Long> uids) {
        AbstractCache<List<Long>>.Node node = new Node(key, uids);
        addToHead(node, false);
        newCache.put(key, node);
        newCount++;
        while (newCount >= newResource) {
            newCache.remove(newTail.prev);
            removeTail(false);
            newCount--;
        }
    }

    public boolean containsCacheKey(long key) {
        return newCache.containsKey(key) || oldCache.containsKey(key);
    }
}
