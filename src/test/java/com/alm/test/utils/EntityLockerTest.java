package com.alm.test.utils;

import com.alm.test.utils.EntityLocker.Lock;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.Set;

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

        Lock lock = getKeyLocks(locker).get(key);
        assertThat(getThreadId(lock), is(Thread.currentThread().getId()));
        assertThat(getReentrantLockCount(lock), is(1));

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

        Lock lock = getKeyLocks(locker).get(key);
        assertThat(getThreadId(lock), is(Thread.currentThread().getId()));
        assertThat(getReentrantLockCount(lock), is(2));

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

        Lock lock = getKeyLocks(locker).get(key);
        assertThat(getThreadId(lock), is(Thread.currentThread().getId()));
        assertThat(getReentrantLockCount(lock), is(1));

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

        Lock lock = keyLocks.get(keyOne);
        assertThat(getThreadId(lock), is(Thread.currentThread().getId()));
        assertThat(getReentrantLockCount(lock), is(1));

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

        Lock lock = keyLocks.get(keyOne);
        assertThat(getThreadId(lock), is(Thread.currentThread().getId()));
        assertThat(getReentrantLockCount(lock), is(1));

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
        Lock lock = getGlobalLock(locker);
        assertThat(getThreadId(lock), is(Thread.currentThread().getId()));
        assertThat(getReentrantLockCount(lock), is(1));
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
        Lock lock = getGlobalLock(locker);
        assertThat(getThreadId(lock), is(Thread.currentThread().getId()));
        assertThat(getReentrantLockCount(lock), is(2));
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
        Lock lock = getGlobalLock(locker);
        assertThat(getThreadId(lock), is(Thread.currentThread().getId()));
        assertThat(getReentrantLockCount(lock), is(1));

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

        Lock lock = getKeyLocks(locker).get(key);
        assertThat(getThreadId(lock), is(Thread.currentThread().getId()));
        assertThat(getReentrantLockCount(lock), is(1));

        assertFalse(getIsLockEscalated(locker).get());
        assertFalse(getIsWaitingForGlobalLock(locker));
        lock = getGlobalLock(locker);
        assertThat(getThreadId(lock), is(Thread.currentThread().getId()));
        assertThat(getReentrantLockCount(lock), is(1));
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
        Lock lock = getGlobalLock(locker);
        assertThat(getThreadId(lock), is(Thread.currentThread().getId()));
        assertThat(getReentrantLockCount(lock), is(1));
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
        Lock lock = getGlobalLock(locker);
        assertThat(getThreadId(lock), is(Thread.currentThread().getId()));
        assertThat(getReentrantLockCount(lock), is(1));

        locker.lock(keyThree);

        assertTrue(getIsLockEscalated(locker).get());
        lock = getGlobalLock(locker);
        assertThat(getThreadId(lock), is(Thread.currentThread().getId()));
        assertThat(getReentrantLockCount(lock), is(1));
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
        Lock lock = getGlobalLock(locker);
        assertThat(getThreadId(lock), is(Thread.currentThread().getId()));
        assertThat(getReentrantLockCount(lock), is(1));
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
        Lock lock = getGlobalLock(locker);
        assertThat(getThreadId(lock), is(Thread.currentThread().getId()));
        assertThat(getReentrantLockCount(lock), is(1));
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