package org.gorttar.concurrent.locks

import java.util.concurrent.TimeUnit.MILLISECONDS
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

/**
 * Utility class that provides synchronization mechanism similar to row-level DB locking.
 * The class is supposed to be used by the components that are responsible for managing storage
 * and caching of different type of entities in the application.
 * [EntityLocker] itself does not deal with the entities, only with the IDs (primary keys) of the entities.
 * [K] is the type of entity ID
 */
class EntityLocker<K : Any> {
    private val keyToLockCount = mutableMapOf<K, LockCount>()
    private val globalLock = ReentrantLock()
    private val globalMonitor = globalLock.newCondition()
    private var globalLockCount: LockCount? = null

    /**
     * executes [protectedCode] exclusively accessing entity determined by [key]
     * ensuring that at most one [Thread] executes protected code on that entity
     * If there’s a concurrent request to lock the same entity, the other thread
     * should wait until the entity becomes available.
     * Concurrent execution of protected code on different entities is allowed.
     * Reentrant invocation is allowed.
     */
    inline fun <T> withLock(key: K, protectedCode: () -> T): T = lock(key).run {
        try {
            protectedCode()
        } finally {
            unlock(key)
        }
    }

    inline fun <T> withTryLock(
        key: K,
        timeout: Long,
        protectedCode: () -> T
    ): TryLockResult<T> = tryLock(key, timeout).takeIf { it }?.run {
        try {
            Success(protectedCode())
        } finally {
            unlock(key)
        }
    } ?: TimeoutExceeded

    /**
     * executes [protectedCode] exclusively
     * ensuring that at most one [Thread] executes protected code on any entity
     * If there’s a concurrent request to lock any entity, the other thread
     * should wait until the entity becomes available.
     * Reentrant invocation is allowed.
     */
    inline fun <T> withGlobalLock(protectedCode: () -> T): T = globalLock().run {
        try {
            protectedCode()
        } finally {
            globalUnlock()
        }
    }

    inline fun <T> withTryGlobalLock(timeout: Long, protectedCode: () -> T): TryLockResult<T> {
        TODO()
    }

    /**
     * Acquires the lock on [key] according to the following strategy:
     * lock on [key] isn't held by another thread -> acquires and returns immediately
     * lock on [key] is already held by [Thread.currentThread] -> increment associated [LockCount]
     *      and returns immediately
     * else -> the current thread becomes disabled for thread scheduling purposes and lies dormant until the locks
     *      has been acquired, at which time the locks hold count is set to one.
     *
     * This fun is internal in order to prevent [EntityLocker] user from lock leaks
     */
    @PublishedApi
    internal fun lock(key: K): Unit = globalLock.withLock {
        while (isLocked(key) || isGloballyLocked()) globalMonitor.await()
        keyToLockCount.computeIfAbsent(key) { LockCount() }.inc()
    }

    /**
     * Attempts to release the lock on [key]
     * This fun is internal in order to prevent [EntityLocker] user from misuse e.g. attempting to release not held lock
     */
    @PublishedApi
    internal fun unlock(key: K): Unit = globalLock.withLock {
        if (keyToLockCount[key]?.thread === Thread.currentThread()) {
            val lockCont = keyToLockCount[key]?.dec()
            if (lockCont?.count == 0) {
                keyToLockCount.remove(key)
                globalMonitor.signalAll()
            }
        }
    }

    @PublishedApi
    internal fun tryLock(key: K, timeout: Long): Boolean = globalLock.withLock {
        if (isLocked(key) || isGloballyLocked()) globalMonitor.await(timeout, MILLISECONDS)
        if (!isLocked(key) && !isGloballyLocked()) {
            keyToLockCount.computeIfAbsent(key) { LockCount() }.inc()
            true
        } else false
    }

    @PublishedApi
    internal fun globalLock(): Unit = globalLock.withLock {
        while (isGloballyLocked() || keyToLockCount.values.any { it.count > 0 }) globalMonitor.await()
        globalLockCount = (globalLockCount ?: LockCount()).inc()
    }

    @PublishedApi
    internal fun globalUnlock(): Unit = globalLock.withLock {
        globalLockCount?.let {
            if (it.thread === Thread.currentThread()) {
                it.dec()
                if (it.count == 0) {
                    globalLockCount = null
                    globalMonitor.signalAll()
                }
            }
        }
    }

    private fun isGloballyLocked() =
        (globalLockCount?.count ?: 0) > 0 && globalLockCount?.thread !== Thread.currentThread()

    private fun isLocked(key: K) =
        (keyToLockCount[key]?.count ?: 0) > 0 && keyToLockCount[key]?.thread !== Thread.currentThread()
}

private class LockCount {
    val thread: Thread = Thread.currentThread()
    var count = 0
        private set

    fun inc() = apply { count++ }
    fun dec() = apply { count-- }
}

sealed class TryLockResult<out V>
data class Success<V>(val v: V) : TryLockResult<V>()
object TimeoutExceeded : TryLockResult<Nothing>()
