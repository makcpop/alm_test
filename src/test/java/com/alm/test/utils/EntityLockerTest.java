package com.alm.test.utils;

import com.alm.test.utils.EntityLocker.Lock;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EntityLockerTest {

    @Test
    public void testSingleLock() throws Exception {
        EntityLocker<Long> locker = new EntityLocker<>();

        long key = 123L;
        locker.lock(key);

        assertThat(getThreadWithLocks(locker), contains(Thread.currentThread().getId()));
        assertThat(getLockedKeys(locker).get(), contains(key));

        checkLock(getKeyLocks(locker).get(key), 1);

        assertFalse(getIsLockEscalated(locker).get());
        assertFalse(getIsWaitingForGlobalLock(locker));
        assertThat(getGlobalLock(locker), nullValue());
    }

    @Test
    public void testReleaseLock() throws Exception {
        EntityLocker<Long> locker = new EntityLocker<>();

        long key = 123L;
        locker.lock(key);
        locker.unlock(key);

        assertThat(getThreadWithLocks(locker), empty());
        assertThat(getLockedKeys(locker).get(), empty());
        assertThat(getKeyLocks(locker), anEmptyMap());

        assertFalse(getIsLockEscalated(locker).get());
        assertFalse(getIsWaitingForGlobalLock(locker));
        assertThat(getGlobalLock(locker), nullValue());
    }

    @Test
    public void testReentrantLock() throws Exception {
        EntityLocker<Long> locker = new EntityLocker<>();

        long key = 123L;
        locker.lock(key);
        locker.lock(key);

        assertThat(getThreadWithLocks(locker), contains(Thread.currentThread().getId()));
        assertThat(getLockedKeys(locker).get(), contains(key));

        checkLock(getKeyLocks(locker).get(key), 2);

        assertFalse(getIsLockEscalated(locker).get());
        assertFalse(getIsWaitingForGlobalLock(locker));
        assertThat(getGlobalLock(locker), nullValue());
    }

    @Test
    public void testReleaseReentrantLock() throws Exception {
        EntityLocker<Long> locker = new EntityLocker<>();

        long key = 123L;
        locker.lock(key);
        locker.lock(key);

        locker.unlock(key);

        assertThat(getThreadWithLocks(locker), contains(Thread.currentThread().getId()));
        assertThat(getLockedKeys(locker).get(), contains(key));

        checkLock(getKeyLocks(locker).get(key), 1);

        locker.unlock(key);

        assertThat(getThreadWithLocks(locker), empty());
        assertThat(getLockedKeys(locker).get(), empty());
        assertThat(getKeyLocks(locker), anEmptyMap());

        assertFalse(getIsLockEscalated(locker).get());
        assertFalse(getIsWaitingForGlobalLock(locker));
        assertThat(getGlobalLock(locker), nullValue());
    }

    @Test
    public void testLockTwoKeys() throws Exception {
        EntityLocker<Long> locker = new EntityLocker<>();

        long keyOne = 123L;
        long keyTwo = 321L;
        locker.lock(keyOne);
        locker.lock(keyTwo);

        assertThat(getThreadWithLocks(locker), contains(Thread.currentThread().getId()));
        assertThat(getLockedKeys(locker).get(), containsInAnyOrder(keyOne, keyTwo));

        Map<Long, Lock> keyLocks = getKeyLocks(locker);

        checkLock(keyLocks.get(keyOne), 1);
        Lock lock;

        lock = keyLocks.get(keyTwo);
        assertThat(getThreadId(lock), is(Thread.currentThread().getId()));
        assertThat(getReentrantLockCount(lock), is(1));

        assertFalse(getIsLockEscalated(locker).get());
        assertFalse(getIsWaitingForGlobalLock(locker));
        assertThat(getGlobalLock(locker), nullValue());
    }

    @Test
    public void testReleaseLockTwoKeys() throws Exception {
        EntityLocker<Long> locker = new EntityLocker<>();

        long keyOne = 123L;
        long keyTwo = 321L;
        locker.lock(keyOne);
        locker.lock(keyTwo);

        locker.unlock(keyTwo);

        assertThat(getThreadWithLocks(locker), contains(Thread.currentThread().getId()));
        assertThat(getLockedKeys(locker).get(), contains(keyOne));

        Map<Long, Lock> keyLocks = getKeyLocks(locker);

        checkLock(keyLocks.get(keyOne), 1);
        Lock lock;

        lock = keyLocks.get(keyTwo);
        assertThat(lock, nullValue());

        locker.unlock(keyOne);

        assertThat(getThreadWithLocks(locker), empty());
        assertThat(getLockedKeys(locker).get(), empty());
        assertThat(getKeyLocks(locker), anEmptyMap());

        assertFalse(getIsLockEscalated(locker).get());
        assertFalse(getIsWaitingForGlobalLock(locker));
        assertThat(getGlobalLock(locker), nullValue());
    }

    @Test
    public void testGlobalLock() throws Exception {
        EntityLocker<Long> locker = new EntityLocker<>();

        locker.lockGlobal();

        assertThat(getThreadWithLocks(locker), empty());
        assertThat(getLockedKeys(locker).get(), empty());
        assertThat(getKeyLocks(locker), anEmptyMap());

        assertFalse(getIsLockEscalated(locker).get());
        assertFalse(getIsWaitingForGlobalLock(locker));
        checkLock(getGlobalLock(locker), 1);
    }

    @Test
    public void testReleaseGlobalLock() throws Exception {
        EntityLocker<Long> locker = new EntityLocker<>();

        locker.lockGlobal();
        locker.unlockGlobal();

        assertThat(getThreadWithLocks(locker), empty());
        assertThat(getLockedKeys(locker).get(), empty());
        assertThat(getKeyLocks(locker), anEmptyMap());

        assertFalse(getIsLockEscalated(locker).get());
        assertFalse(getIsWaitingForGlobalLock(locker));
        assertThat(getGlobalLock(locker), nullValue());
    }

    @Test
    public void testReentrantGlobalLock() throws Exception {
        EntityLocker<Long> locker = new EntityLocker<>();

        locker.lockGlobal();
        locker.lockGlobal();

        assertThat(getThreadWithLocks(locker), empty());
        assertThat(getLockedKeys(locker).get(), empty());
        assertThat(getKeyLocks(locker), anEmptyMap());

        assertFalse(getIsLockEscalated(locker).get());
        assertFalse(getIsWaitingForGlobalLock(locker));
        checkLock(getGlobalLock(locker), 2);
    }

    @Test
    public void testReleaseReentrantGlobalLock() throws Exception {
        EntityLocker<Long> locker = new EntityLocker<>();

        locker.lockGlobal();
        locker.lockGlobal();

        locker.unlockGlobal();

        assertThat(getThreadWithLocks(locker), empty());
        assertThat(getLockedKeys(locker).get(), empty());
        assertThat(getKeyLocks(locker), anEmptyMap());

        assertFalse(getIsWaitingForGlobalLock(locker));
        checkLock(getGlobalLock(locker), 1);

        locker.unlockGlobal();

        assertFalse(getIsLockEscalated(locker).get());
        assertFalse(getIsWaitingForGlobalLock(locker));
        assertThat(getGlobalLock(locker), nullValue());
    }

    @Test
    public void testEntityLockUnderGlobalLock() throws Exception {
        EntityLocker<Long> locker = new EntityLocker<>();

        long key = 123L;

        locker.lockGlobal();
        locker.lock(key);

        assertThat(getThreadWithLocks(locker), contains(Thread.currentThread().getId()));
        assertThat(getLockedKeys(locker).get(), contains(key));

        checkLock(getKeyLocks(locker).get(key), 1);
        Lock lock;

        assertFalse(getIsLockEscalated(locker).get());
        assertFalse(getIsWaitingForGlobalLock(locker));
        checkLock(getGlobalLock(locker), 1);
    }

    @Test
    public void testReleaseEntityLockUnderGlobalLock() throws Exception {
        EntityLocker<Long> locker = new EntityLocker<>();

        long key = 123L;

        locker.lockGlobal();
        locker.lock(key);
        locker.unlock(key);

        assertThat(getThreadWithLocks(locker), empty());
        assertThat(getLockedKeys(locker).get(), empty());
        assertThat(getKeyLocks(locker), anEmptyMap());

        assertFalse(getIsWaitingForGlobalLock(locker));
        checkLock(getGlobalLock(locker), 1);
    }

    @Test
    public void testEscalateLockToGlobal() throws Exception {
        EntityLocker<Long> locker = new EntityLocker<>(2);

        long keyOne = 123L;
        long keyTwo = 321L;
        long keyThree = 777L;
        locker.lock(keyOne);

        assertFalse(getIsLockEscalated(locker).get());
        assertFalse(getIsWaitingForGlobalLock(locker));
        assertThat(getGlobalLock(locker), nullValue());

        locker.lock(keyTwo);

        assertTrue(getIsLockEscalated(locker).get());
        checkLock(getGlobalLock(locker), 1);

        locker.lock(keyThree);

        assertTrue(getIsLockEscalated(locker).get());
        checkLock(getGlobalLock(locker), 1);
    }

    @Test
    public void testDeescalateGlobalLock() throws Exception {
        EntityLocker<Long> locker = new EntityLocker<>(2);

        long keyOne = 123L;
        long keyTwo = 321L;

        locker.lock(keyOne);
        locker.lock(keyTwo);
        locker.unlock(keyTwo);

        assertFalse(getIsLockEscalated(locker).get());
        assertFalse(getIsWaitingForGlobalLock(locker));
        assertThat(getGlobalLock(locker), nullValue());
    }

    @Test
    public void testEscalateLockToGlobal_globalAlreadyLocked() throws Exception {
        EntityLocker<Long> locker = new EntityLocker<>(2);

        long keyOne = 123L;
        long keyTwo = 321L;

        locker.lockGlobal();

        locker.lock(keyOne);
        locker.lock(keyTwo);

        assertFalse(getIsLockEscalated(locker).get());
        checkLock(getGlobalLock(locker), 1);
    }

    @Test
    public void testDeescalateGlobalLock_globalAlreadyLocked() throws Exception {
        EntityLocker<Long> locker = new EntityLocker<>(2);

        long keyOne = 123L;
        long keyTwo = 321L;

        locker.lockGlobal();

        locker.lock(keyOne);
        locker.lock(keyTwo);
        locker.unlock(keyTwo);

        assertFalse(getIsLockEscalated(locker).get());
        checkLock(getGlobalLock(locker), 1);
    }

    @Test
    public void testTryLock_simpleTest() throws Exception {
        EntityLocker<Long> locker = new EntityLocker<>(2);

        assertTrue(locker.tryLock(1L));
    }

    @Test
    public void testTryLock_withExistingGlobalLock() throws Exception {
        EntityLocker<Long> locker = new EntityLocker<>(2);

        locker.lock(2L);
        locker.lock(3L);

        assertTrue(locker.tryLock(1L));
    }

    @Test
    public void testTryLock_holdGlobalLock() throws Exception {
        EntityLocker<Long> locker = new EntityLocker<>(2);

        locker.lock(2L);

        assertTrue(locker.tryLock(1L));
        checkLock(getGlobalLock(locker), 1);
    }

    @Test
    public void testTryLock_reentrantLock() throws Exception {
        EntityLocker<Long> locker = new EntityLocker<>(2);

        long key = 1L;
        locker.lock(key);
        assertTrue(locker.tryLock(key));
        checkLock(getKeyLocks(locker).get(key), 2);
    }

    @Test
    public void testTryLock_alreadyLocked() throws Exception {
        EntityLocker<Long> locker = new EntityLocker<>(2);
        long key = 1L;

        CountDownLatch latch = new CountDownLatch(1);

        new Thread(() -> {
            try {
                locker.lock(key);
                latch.countDown();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }).start();

        latch.await();

        assertFalse(locker.tryLock(key));
    }

    @Test
    public void testTryLockWithTimeout_simpleTest() throws Exception {
        EntityLocker<Long> locker = new EntityLocker<>(2);

        assertTrue(locker.tryLock(1L, 1000L));
    }

    @Test
    public void testTryLockWithTimeout_withExistingGlobalLock() throws Exception {
        EntityLocker<Long> locker = new EntityLocker<>(2);

        locker.lock(2L);
        locker.lock(3L);

        assertTrue(locker.tryLock(1L, 1000L));
    }

    @Test
    public void testTryLockWithTimeout_holdGlobalLock() throws Exception {
        EntityLocker<Long> locker = new EntityLocker<>(2);

        locker.lock(2L);

        assertTrue(locker.tryLock(1L, 1000L));
        checkLock(getGlobalLock(locker), 1);
    }

    @Test
    public void testTryLockWithTimeout_reentrantLock() throws Exception {
        EntityLocker<Long> locker = new EntityLocker<>(2);

        long key = 1L;
        locker.lock(key);
        assertTrue(locker.tryLock(key, 1000L));
        checkLock(getKeyLocks(locker).get(key), 2);
    }

    @Test
    public void testTryLockWithTimeout_alreadyLocked() throws Exception {
        EntityLocker<Long> locker = new EntityLocker<>(2);
        long key = 1L;

        CountDownLatch latch = new CountDownLatch(1);

        new Thread(() -> {
            try {
                locker.lock(key);
                latch.countDown();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }).start();

        latch.await();

        assertFalse(locker.tryLock(key, 1000L));
    }

    private void checkLock(Lock globalLock, int reentrantCount) throws Exception {
        Lock lock = globalLock;
        assertThat(getThreadId(lock), is(Thread.currentThread().getId()));
        assertThat(getReentrantLockCount(lock), is(reentrantCount));
    }

    @Test
    public void testIsLocked() throws Exception {
        long key = 1L;
        EntityLocker<Long> locker = new EntityLocker<>();
        assertFalse(locker.isLocked(key));
        locker.lock(key);
        assertTrue(locker.isLocked(key));

    }

    private Lock getGlobalLock(EntityLocker<Long> entityLocker) throws Exception{
        Field keyLocks = EntityLocker.class.getDeclaredField("globalLock");
        keyLocks.setAccessible(true);
        return (Lock) keyLocks.get(entityLocker);
    }

    private ThreadLocal<Boolean> getIsLockEscalated(EntityLocker<Long> entityLocker) throws Exception{
        Field keyLocks = EntityLocker.class.getDeclaredField("isLockEscalated");
        keyLocks.setAccessible(true);
        return (ThreadLocal<Boolean>) keyLocks.get(entityLocker);
    }

    private boolean getIsWaitingForGlobalLock(EntityLocker<Long> entityLocker) throws Exception{
        Field keyLocks = EntityLocker.class.getDeclaredField("isWaitingForGlobalLock");
        keyLocks.setAccessible(true);
        return (boolean) keyLocks.get(entityLocker);
    }

    private long getThreadId(Lock lock) throws Exception{
        Field field = Lock.class.getDeclaredField("threadId");
        field.setAccessible(true);
        return (long)(field.get(lock));
    }

    private int getReentrantLockCount(Lock lock) throws Exception{
        Field field = Lock.class.getDeclaredField("reentrantLockCount");
        field.setAccessible(true);
        return (int)(field.get(lock));
    }

    private Map<Long, Lock> getKeyLocks(EntityLocker<Long> entityLocker) throws Exception {
        Field keyLocks = EntityLocker.class.getDeclaredField("keyLocks");
        keyLocks.setAccessible(true);
        return (Map<Long, Lock>) keyLocks.get(entityLocker);
    }

    private Set<Long> getThreadWithLocks(EntityLocker<Long> entityLocker) throws Exception {
        Field field = EntityLocker.class.getDeclaredField("threadsWithLocks");
        field.setAccessible(true);
        return (Set<Long>) field.get(entityLocker);
    }

    private ThreadLocal<Set<Long>> getLockedKeys(EntityLocker<Long> entityLocker) throws Exception {
        Field field = EntityLocker.class.getDeclaredField("lockedKeys");
        field.setAccessible(true);
        return (ThreadLocal<Set<Long>>) field.get(entityLocker);
    }
}