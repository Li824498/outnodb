package server.common;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public abstract class AbstractCache<T> {
    protected int ageDiff;
    protected int maxResource;
    Set<Long> getting;
    Lock lock;

    protected Map<Long, Node> newCache = new HashMap<>();
    Node newHead;
    protected Node newTail;
    protected int newResource;
    protected int newCount;

    protected Map<Long, Node> oldCache = new HashMap<>();
    Node oldHead;
    Node oldTail;
    int oldResource;
    int oldCount;

    protected class Node {
        protected Long key;
        protected T content;
        Long state;
        public Node prev;
        Node next;

        public Node(Long key, T content) {
            this.key = key;
            this.content = content;
            state = 1L;
        }

        public Node() {}

        public T getContent() {
            return content;
        }
    }



    public AbstractCache(int maxResource) {
        ageDiff = 10;
        this.maxResource = maxResource;
        getting = new HashSet<>();
        lock = new ReentrantLock();
        newResource = (int) (maxResource * 0.3);
        oldResource = (int) (maxResource * 0.7);
        newCount = 0;
        oldCount = 0;

        newHead = new Node();
        newTail = new Node();
        oldHead = new Node();
        oldTail = new Node();

        oldHead.next = oldTail;
        oldTail.prev = oldHead;
        newHead.next = newTail;
        newTail.prev = newHead;
    }


    protected T get(long key) {
        while (true) {
            lock.lock();
            if(getting.contains(key)) {
                lock.unlock();
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
//                    continue;
                }
                continue;
            }

            if(oldCache.containsKey(key)) {
                Node node = oldCache.get(key);
                moveToHead(node, true);
                return oldCache.get(key).content;
            } else if(newCache.containsKey(key)) {
                Node node = newCache.get(key);
                moveToHead(node, false);
                node.state++;
                if(node.state > ageDiff) {
                    toOld(node);
                    newCache.remove(key);
                    oldCache.put(key, node);
                }
                return newCache.get(key).content;
            }

            while(newCount >= newResource) {
                newCache.remove(newTail.prev.key);
                newCache.remove(newTail.prev);
                removeTail(true);
                newCount--;
            }

            newCount++;
            getting.add(key);
            lock.unlock();
            break;
        }

        Node node;
        try {
            node = new Node(key, getForCache(key));

        } catch (Exception e) {
            lock.lock();
            newCount--;
            getting.remove(key);
            lock.unlock();
            throw new RuntimeException(e);
        }

        lock.lock();
        getting.remove(key);
        addToHead(node,  true);
        newCache.put(key, node);
        lock.unlock();

        return node.content;
    }

    protected void addToHead(Node node, boolean b) {
        if(b) {
            oldTail.prev.next = node;
            node.prev = oldTail.prev;
            oldTail.prev = node;
            node.next = oldTail;

        } else {
            newTail.prev.next = node;
            node.prev = newTail.prev;
            newTail.prev = node;
            node.next = newTail;

        }
    }

    protected void removeTail(boolean b) {
        if(b) {
            removeNode(oldTail.prev);
        } else {
            removeNode(newTail.prev);
        }
    }

    private void toOld(Node node) {
        // 1.升级进入老年代
        // 2.老年代满了就弹出老人
        removeNode(node);
        addToHead(node, true);
    }

    protected void moveToHead(Node node, Boolean b) {
        removeNode(node);
        addToHead(node, b);

    }

    protected void release(long key) {
        lock.lock();
        Node node;
        if(oldCache.containsKey(key)) {
            node = oldCache.get(key);
            oldCache.remove(key);
            oldCount--;
        } else {
            node = newCache.get(key);
            newCache.remove(key);
            newCount--;
        }
        try {
            releaseForCache(node.content);
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            lock.unlock();
        }
    }

    protected void close() {
        lock.lock();
        try {
            for(Long key : newCache.keySet()) {
                Node node = newCache.get(key);
                removeNode(node);
                releaseForCache(node.content);
                newResource--;
            }
            for(Long key : oldCache.keySet()) {
                Node node = oldCache.get(key);
                removeNode(node);
                releaseForCache(node.content);
                oldResource--;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            lock.unlock();
        }
    }

    private void removeNode(Node node) {
        node.prev.next = node.next;
        node.next.prev = node.prev;
    }

    protected abstract T getForCache(long key) throws Exception;

    protected abstract void releaseForCache(T obj);


}
