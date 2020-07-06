package com.alm.test.utils;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * The utility class that provides synchronization mechanism similar to row-level DB locking.
 * EntityLocker itself does not deal with the entities, only with the IDs of the entities.
 * EntityLocker allow to executed protected code in thread safe way.
 *
 * EntityLocker supports reentrant lock.
 *
 * If 'escalationThreshold' is set, the lock will be escalated to global lock if the thread lock >= than
 * escalationThreshold entities. Escalated global lock will be released when number of locked entity is less than
 * escalationThreshold.
 *
 * Escalation isn't happened if global lock is already acquired or another thread waiting for acquiring global lock.
 * Escalated lock allow to acquire global lock by the same thread.
 *
 * @param <T>
 */
public class EntityLocker<T> {
    private final Map<T, Lock> keyLocks = new HashMap<>();
    private final Set<Long> threadsWithLocks = new HashSet<>();
    private final ThreadLocal<Set<T>> lockedKeys = ThreadLocal.withInitial(HashSet::new);

    private final ThreadLocal<Boolean> isLockEscalated = ThreadLocal.withInitial(() -> false);
    private final int escalationThreshold;

    private boolean isWaitingForGlobalLock = false;
    private Lock globalLock;

    public EntityLocker(int escalationThreshold) {
        this.escalationThreshold = escalationThreshold;
    }

    public EntityLocker() {
        this.escalationThreshold = Integer.MAX_VALUE;
    }

    /**
     * The method acquire the specified entity.
     * Support reentrant lock.
     *
     * Lock can be escalated to global if locks count isn't less than escalationThreshold.
     *
     * @param key
     * @throws InterruptedException
     */
    public synchronized void lock(T key) throws InterruptedException {
        if (key == null) {
            throw new IllegalArgumentException("Key is null");
        }

        while ((isWaitingForGlobalLock && !threadsWithLocks.contains(threadId()))
                || (globalLock != null && !globalLock.acquiredByCurrentThread())) {
            wait();
        }

        Lock lock = keyLocks.computeIfAbsent(key, k -> createLock());
        while (!lock.tryAcquire()) {
            wait();
            lock = keyLocks.computeIfAbsent(key, k -> createLock());
        }
        lockedKeys.get().add(key);
        threadsWithLocks.add(threadId());

        escalateLockIfRequired();

        notifyAll();
    }

    /**
     * The method release the specified entity.
     *
     * Global lock can be deescalated to locks if locks count is less than escalationThreshold.
     *
     * @param key
     * @throws IllegalMonitorStateException if key not locked or locked by another thread.
     */
    public synchronized void unlock(T key) {
        if (key == null) {
            throw new IllegalArgumentException("Key is null");
        }

        Lock lock = keyLocks.get(key);
        if (lock == null || !lock.tryRelease()) {
            throw new IllegalMonitorStateException();
        }

        if (lock.isReleased()) {
            keyLocks.remove(key);
            lockedKeys.get().remove(key);
            if (lockedKeys.get().isEmpty()) {
                threadsWithLocks.remove(threadId());
            }
        }

        deescalateLockIfRequired();

        notifyAll();
    }

    /**
     * Acquire global lock. Thread will work with locks in exclusive manner.
     *
     * Before acquiring global lock, the locker wait for realising all entities already acquired by another thread.
     * Doesn't allow to acquire lock by new thread.
     *
     * @throws InterruptedException
     */
    public synchronized void lockGlobal() throws InterruptedException {
        while (isWaitingForGlobalLock ||
                (globalLock != null && !globalLock.acquiredByCurrentThread())) {
            wait();
        }

        if (globalLock == null) {
            try {
                isWaitingForGlobalLock = true;
                while (isOtherThreadsHaveLocks()) {
                    wait();
                }
            } catch (InterruptedException e) {
                isWaitingForGlobalLock = false;
                notifyAll();
                throw e;
            }
            globalLock = createLock();
            isWaitingForGlobalLock = false;
        }

        globalLock.tryAcquire();
        notifyAll();
    }

    /**
     * Release global lock.
     *
     * @throws IllegalMonitorStateException if global lok not acquired or acquired by another thread
     */
    public synchronized void unlockGlobal() {
        if (globalLock == null
                || (isLockEscalated.get() && globalLock.reentrantLockCount == 1)
                || !globalLock.tryRelease()) {
            throw new IllegalMonitorStateException();
        }

        if (globalLock.isReleased()) {
            globalLock = null;
        }
        notifyAll();
    }

    private void escalateLockIfRequired() throws InterruptedException {
        if (lockedKeys.get().size() < escalationThreshold || isLockEscalated.get()) {
            return;
        }
        if (isWaitingForGlobalLock || globalLock != null) {
            return;
        }

        try {
            isWaitingForGlobalLock = true;
            while (isOtherThreadsHaveLocks()) {
                wait();
            }
        } catch (InterruptedException e) {
            isWaitingForGlobalLock = false;
            throw e;
        }
        globalLock = createLock();
        isWaitingForGlobalLock = false;

        globalLock.tryAcquire();

        isLockEscalated.set(true);
    }

    private void deescalateLockIfRequired() {
        if (lockedKeys.get().size() >= escalationThreshold || !isLockEscalated.get()) {
            return;
        }

        globalLock.tryRelease();

        if (globalLock.isReleased()) {
            globalLock = null;
        }

        isLockEscalated.set(false);
    }

    private boolean isOtherThreadsHaveLocks() {
        return !threadsWithLocks.isEmpty() &&
                !(threadsWithLocks.size() == 1 && threadsWithLocks.contains(threadId()));
    }

    private Lock createLock() {
        return new Lock(Thread.currentThread().getId(), 0);
    }

    private static long threadId() {
        return Thread.currentThread().getId();
    }

    public class Lock {
        private final long threadId;
        private int reentrantLockCount;

        public Lock(long threadId, int reentrantLockCount) {
            this.threadId = threadId;
            this.reentrantLockCount = reentrantLockCount;
        }

        public long getThreadId() {
            return threadId;
        }

        public int getReentrantLockCount() {
            return reentrantLockCount;
        }

        public boolean tryAcquire() {
            if (acquiredByCurrentThread()) {
                reentrantLockCount++;
                return true;
            }
            return false;
        }

        public boolean tryRelease() {
            if (acquiredByCurrentThread()) {
                reentrantLockCount--;
                return true;
            }
            return false;
        }

        public boolean acquiredByCurrentThread() {
            return threadId == Thread.currentThread().getId();
        }

        public boolean isReleased() {
            return reentrantLockCount == 0;
        }
    }
}
