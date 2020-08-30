package com.alm.test.utils;

import java.time.Instant;
import java.util.*;

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
    private final Map<Long, T> threadWaitingForLocks = new HashMap<>();
    private final Set<Long> threadsWithLocks = new HashSet<>();
    private final ThreadLocal<Set<T>> lockedKeys = ThreadLocal.withInitial(HashSet::new);

    private final ThreadLocal<Boolean> isLockEscalated = ThreadLocal.withInitial(() -> false);
    private final int escalationThreshold;

    private final long waitingTimeout = 100;

    private Long waitingForGlobalLock;
    private Lock globalLock;

    public EntityLocker(int escalationThreshold) {
        this.escalationThreshold = escalationThreshold;
    }

    public EntityLocker() {
        this.escalationThreshold = Integer.MAX_VALUE;
    }

    /**
     * The method acquire the specified entity. The method is synchronized.
     * Support reentrant lock.
     *
     * Lock can be escalated to global if locks count isn't less than escalationThreshold.
     *
     * @param key
     * @throws InterruptedException throws in case of the thread is interrupted. No one already acquired locked is released,
     * they should be released manually.
     * @throws DeadLockException throws in case of deadlock is detected. No one already acquired locked is released,
     * they should be released manually.
     */
    public synchronized void lock(T key) throws InterruptedException {
        if (key == null) {
            throw new IllegalArgumentException("Key is null");
        }

        while ((waitingForGlobalLock != null && !threadsWithLocks.contains(threadId()))
                || (globalLock != null && !globalLock.acquiredByCurrentThread())) {
            wait();
        }

        try {
            threadWaitingForLocks.put(threadId(), key);
            Lock lock = keyLocks.computeIfAbsent(key, k -> createLock());
            boolean isFirstIteration = true;
            while (!lock.tryAcquire()) {
                if (!isFirstIteration) {
                    checkForDeadlock(key);
                }
                wait(waitingTimeout);
                isFirstIteration = false;
                lock = keyLocks.computeIfAbsent(key, k -> createLock());
            }
            lockedKeys.get().add(key);
            threadsWithLocks.add(threadId());
        }
        finally {
            threadWaitingForLocks.remove(threadId());
        }

        escalateLockIfRequired();

        notifyAll();
    }

    /**
     * The method try to acquire the specified entity. The method doesn't have any waiting. The method is synchronized.
     * Support reentrant lock.
     *
     * Lock can be escalated to global if locks count isn't less than escalationThreshold and no any waiting is required.
     *
     * @param key
     * @throws DeadLockException throws in case of deadlock is detected. No one already acquired locked is released,
     * they should be released manually.
     */
    public synchronized boolean tryLock(T key) {
        if (key == null) {
            throw new IllegalArgumentException("Key is null");
        }

        if ((waitingForGlobalLock != null && !threadsWithLocks.contains(threadId()))
                || (globalLock != null && !globalLock.acquiredByCurrentThread())) {
            return false;
        }

        Lock lock = keyLocks.computeIfAbsent(key, k -> createLock());
        if (!lock.tryAcquire()) {
            checkForDeadlock(key);
            return false;
        }
        lockedKeys.get().add(key);
        threadsWithLocks.add(threadId());

        tryEscalateLockIfRequired();

        notifyAll();
        return true;
    }

    /**
     * The method try to acquire the specified entity. If any thread hold global or the key lock, method will wait for
     * specified timeout. The method is synchronized. Support reentrant lock.
     *
     * Lock can be escalated to global if locks count isn't less than escalationThreshold and lock can be hold in
     * specified timeout.
     *
     * @param key
     * @throws InterruptedException throws in case of the thread is interrupted. No one already acquired locked is released,
     * they should be released manually.
     * @throws DeadLockException throws in case of deadlock is detected. No one already acquired locked is released,
     * they should be released manually.
     */
    public synchronized boolean tryLock(T key, long timeout) throws InterruptedException {
        if (key == null) {
            throw new IllegalArgumentException("Key is null");
        }

        long tryUntil = Instant.now().toEpochMilli() + timeout;

        while ((waitingForGlobalLock != null && !threadsWithLocks.contains(threadId()))
                || (globalLock != null && !globalLock.acquiredByCurrentThread())) {
            long currentTimeout = getTimeout(tryUntil);
            if (currentTimeout <= 0) {
                return false;
            }
            wait(currentTimeout);
        }

        threadWaitingForLocks.put(threadId(), key);
        try {
            Lock lock = keyLocks.computeIfAbsent(key, k -> createLock());
            boolean isFirstIteration = true;
            while (!lock.tryAcquire()) {
                if (!isFirstIteration) {
                    checkForDeadlock(key);
                }

                long currentTimeout = getTimeout(tryUntil);
                if (currentTimeout <= 0) {
                    return false;
                }

                wait(currentTimeout);
                isFirstIteration = false;
                lock = keyLocks.computeIfAbsent(key, k -> createLock());
            }
            lockedKeys.get().add(key);
            threadsWithLocks.add(threadId());
        }
        finally {
            threadWaitingForLocks.remove(threadId());
        }

        tryEscalateLockIfRequired(tryUntil);

        notifyAll();
        return true;
    }

    /**
     * The method release the specified key.
     *
     * Global lock can be deescalated to locks if locks count is less than escalationThreshold.
     *
     * @param key
     * @throws IllegalMonitorStateException if key not locked or locked by another thread. No one already acquired
     * locked is released, they should be released manually.
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
        while (waitingForGlobalLock != null ||
                (globalLock != null && !globalLock.acquiredByCurrentThread())) {
            wait();
        }

        if (globalLock == null) {
            try {
                waitingForGlobalLock = threadId();
                while (isOtherThreadsHaveLocks()) {
                    wait();
                }
            } catch (InterruptedException e) {
                waitingForGlobalLock = null;
                notifyAll();
                throw e;
            }
            globalLock = createLock();
            waitingForGlobalLock = null;
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

    /**
     * The method release all key locked by current thread.
     *
     * Global lock will be deescalated if acquired.
     *
     * @throws IllegalMonitorStateException if key not locked or locked by another thread. No one already acquired
     * locked is released, they should be released manually.
     */
    public synchronized void unlockAll() {
        for (T key : lockedKeys.get()) {
            Lock lock = keyLocks.get(key);
            if (lock == null) {
                continue;
            }

            keyLocks.remove(key);
        }
        lockedKeys.get().clear();
        threadsWithLocks.remove(threadId());

        deescalateLockIfRequired();

        notifyAll();
    }

    /**
     * Queries if this lock is held by any thread. This method is
     * designed for use in monitoring of the system state,
     * not for synchronization control.
     *
     * @param key key
     * @return {@code true} if any thread holds this lock and
     *         {@code false} otherwise
     */
    public boolean isLocked(T key) {
        return keyLocks.containsKey(key);
    }

    private long getTimeout(long tryUntil) {
        return Long.min(waitingTimeout, tryUntil - Instant.now().toEpochMilli());
    }

    private void checkForDeadlock(T key) {
        long currentThreadId = threadId();
        List<Long> checkedThreads = new ArrayList<>(threadsWithLocks.size());

        while (true) {
            Lock lock = keyLocks.get(key);
            if (lock == null || checkedThreads.contains(lock.getThreadId())) {
                //the key, wanted to hold by some thread, is not hold by any thread, there is no deadlock
                //OR
                //the key is hold by already checked thread (not the current thread), it means that the cycle is found,
                // but current thread is not the cycle. The deadlock exception will be thrown in another thread.
                return;
            }

            checkedThreads.add(lock.getThreadId());

            if (lock.getThreadId() == currentThreadId || Objects.equals(waitingForGlobalLock, currentThreadId)) {
                // cycle is found. It's a deadlock
                throw new DeadLockException();
            }

            key = threadWaitingForLocks.get(lock.getThreadId());
            if (key == null) {
                //the thread don't wait for some key. no cycle.
                return;
            }
        }
    }

    private void escalateLockIfRequired() throws InterruptedException {
        if (lockedKeys.get().size() < escalationThreshold || isLockEscalated.get()) {
            return;
        }
        if (waitingForGlobalLock != null || globalLock != null) {
            return;
        }

        try {
            waitingForGlobalLock = threadId();
            while (isOtherThreadsHaveLocks()) {
                wait();
            }
        }
        finally {
            waitingForGlobalLock = null;
        }
        globalLock = createLock();
        waitingForGlobalLock = null;

        globalLock.tryAcquire();

        isLockEscalated.set(true);
    }

    private void tryEscalateLockIfRequired(long tryUntil) throws InterruptedException {
        if (lockedKeys.get().size() < escalationThreshold || isLockEscalated.get()) {
            return;
        }
        if (waitingForGlobalLock != null || globalLock != null) {
            return;
        }

        try {
            waitingForGlobalLock = threadId();
            while (isOtherThreadsHaveLocks()) {
                long currentTimeout = getTimeout(tryUntil);
                if (currentTimeout <= 0) {
                    return;
                }
                wait(currentTimeout);
            }
        }
        finally {
            waitingForGlobalLock = null;
        }
        globalLock = createLock();
        waitingForGlobalLock = null;

        globalLock.tryAcquire();

        isLockEscalated.set(true);
    }

    private void tryEscalateLockIfRequired() {
        if (lockedKeys.get().size() < escalationThreshold || isLockEscalated.get()) {
            return;
        }
        if (waitingForGlobalLock != null || globalLock != null) {
            return;
        }
        if (isOtherThreadsHaveLocks()) {
            return;
        }

        globalLock = createLock();
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

    public static class Lock {
        private final long threadId;
        private int reentrantLockCount;

        public Lock(long threadId, int reentrantLockCount) {
            this.threadId = threadId;
            this.reentrantLockCount = reentrantLockCount;
        }

        public long getThreadId() {
            return threadId;
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
