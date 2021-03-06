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
 *
 * Implemented requirements:
 * *    [EntityLocker] should support different types of entity IDs.
 * *        @see [K] type parameter
 * *    [EntityLocker] interface should allow the caller to specify which entity does it want
 * *        to work with (using entity ID), and designate the boundaries of the code that should have exclusive access
 * *        to the entity (called “protected code”).
 * *        @see [withLock], [withTryLock]
 * *    For any given entity, [EntityLocker] should guarantee that at most one thread executes
 * *        protected code on that entity.
 * *        If there’s a concurrent request to lock the same entity, the other thread should wait
 * *        until the entity becomes available.
 * *    [EntityLocker] should allow concurrent execution of protected code on different entities.
 *
 * Implemented bonus requirements:
 * *    Allow reentrant locking.
 * *    Allow the caller to specify timeout for locking an entity.
 * *        @see [withTryLock], [withTryGlobalLock]
 * *    Implement global lock. Protected code that executes under a global lock must not execute concurrently
 * *        with any other protected code.
 * *        @see [withGlobalLock], [withTryGlobalLock]
 * *    Implement lock escalation. If a single thread has locked too many entities,
 * *    escalate its lock to be a global lock.
 * *        @see [globalLockThreshold] and related code
 *
 * Not implemented:
 * *    Implement protection from deadlocks (but not taking into account possible locks outside [EntityLocker]).
 */
class EntityLocker<K : Any>(private val globalLockThreshold: Int = 10) {
    private val keyToLockCount = mutableMapOf<K, LockCount>()
    private val threadToLockedKeys = mutableMapOf<Thread, MutableSet<K>>()
    private val globalLock = ReentrantLock()
    private val globalMonitor = globalLock.newCondition()
    private var globalLockCount: LockCount? = null

    /**
     * executes [protectedCode] exclusively accessing entity determined by [key]
     * ensuring that at most one [Thread] executes protected code on that entity
     * If there’s a concurrent request to lock the same entity, the other thread
     * should wait until the entity becomes available.
     * @return the result of execution
     */
    inline fun <T> withLock(key: K, protectedCode: () -> T): T = lock(key).run {
        try {
            protectedCode()
        } finally {
            unlock(key)
        }
    }

    /**
     * tries to start execution of [protectedCode] exclusively accessing entity determined by [key]
     * within at most [timeout] milliseconds
     * ensuring that at most one [Thread] executes protected code on that entity
     * If there’s a concurrent request to lock the same entity, the other thread
     * should wait until the entity becomes available.
     *
     * @return
     * *    [protectedCode] execution was started -> [Success] instance wrapping the result
     * *    unable to start execution during [timeout] -> [TimeoutExceeded] object
     */
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
     * @return the result of execution
     */
    inline fun <T> withGlobalLock(protectedCode: () -> T): T = globalLock().run {
        try {
            protectedCode()
        } finally {
            globalUnlock()
        }
    }

    /**
     * tries to start execution of [protectedCode] exclusively
     * within at most [timeout] milliseconds
     * ensuring that at most one [Thread] executes protected code on any entity
     * If there’s a concurrent request to lock any entity, the other thread
     * should wait until the entity becomes available.
     *
     * @return
     * *    [protectedCode] execution was started -> [Success] instance wrapping the result
     * *    unable to start execution during [timeout] -> [TimeoutExceeded] object
     */
    inline fun <T> withTryGlobalLock(
        timeout: Long,
        protectedCode: () -> T
    ): TryLockResult<T> = tryGlobalLock(timeout).takeIf { it }?.run {
        try {
            Success(protectedCode())
        } finally {
            globalUnlock()
        }
    } ?: TimeoutExceeded

    /**
     * This fun is internal in order to prevent [EntityLocker] user from lock leaks
     */
    @PublishedApi
    internal fun lock(key: K): Unit = globalLock.withLock {
        while (isLocked(key) || isGloballyLocked()) globalMonitor.await()
        if (isTooManyLockedKeys()) globalLock()
        else acquireLock(key)
    }

    /**
     * This fun is internal in order to prevent [EntityLocker] user from misuse e.g. attempting to release not held lock
     */
    @PublishedApi
    internal fun unlock(key: K): Unit = globalLock.withLock {
        when {
            isGloballyLocked() -> globalUnlock()
            keyToLockCount[key]?.thread === Thread.currentThread() -> {
                val lockCont = keyToLockCount[key]?.dec()
                if (lockCont?.count == 0) {
                    keyToLockCount -= key
                    threadToLockedKeys.computeIfPresent(Thread.currentThread()) { _, keys ->
                        keys -= key
                        keys.takeIf { it.isNotEmpty() }
                    }
                    globalMonitor.signalAll()
                }
            }
        }
    }

    /**
     * This fun is internal in order to prevent [EntityLocker] user from lock leaks
     */
    @PublishedApi
    internal fun tryLock(key: K, timeout: Long): Boolean = globalLock.withLock {
        val start = System.currentTimeMillis()
        if (awaitFor(timeout) { !isLocked(key) && !isGloballyLocked() }) {
            if (isTooManyLockedKeys()) {
                val remainingTime = timeout - System.currentTimeMillis() + start
                if (remainingTime > 0) tryGlobalLock(remainingTime) else false
            } else {
                acquireLock(key)
                true
            }
        } else false
    }

    /**
     * This fun is internal in order to prevent [EntityLocker] user from lock leaks
     */
    @PublishedApi
    internal fun globalLock(): Unit = globalLock.withLock {
        while (isGloballyLocked() || isAnyLocked()) globalMonitor.await()
        acquireGlobalLock()
    }

    /**
     * This fun is internal in order to prevent [EntityLocker] user from misuse e.g. attempting to release not held lock
     */
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

    /**
     * This fun is internal in order to prevent [EntityLocker] user from lock leaks
     */
    @PublishedApi
    internal fun tryGlobalLock(timeout: Long): Boolean = globalLock.withLock {
        if (awaitFor(timeout) { !isGloballyLocked() && !isAnyLocked() }) {
            acquireGlobalLock()
            true
        } else false
    }

    private fun isTooManyLockedKeys() = threadToLockedKeys[Thread.currentThread()].orEmpty().size >= globalLockThreshold

    private fun acquireGlobalLock() {
        keyToLockCount.clear()
        threadToLockedKeys.clear()
        globalLockCount = (globalLockCount ?: LockCount()).inc()
    }

    private fun acquireLock(key: K) {
        keyToLockCount.computeIfAbsent(key) { LockCount() }.inc()
        threadToLockedKeys.computeIfAbsent(Thread.currentThread()) { mutableSetOf() } += key
    }

    private fun isAnyLocked() = keyToLockCount.values.any { it.count > 0 }

    private fun isGloballyLocked() =
        (globalLockCount?.count ?: 0) > 0 && globalLockCount?.thread !== Thread.currentThread()

    private fun isLocked(key: K) =
        (keyToLockCount[key]?.count ?: 0) > 0 && keyToLockCount[key]?.thread !== Thread.currentThread()

    private inline fun awaitFor(timeout: Long, predicate: () -> Boolean): Boolean {
        val start = System.currentTimeMillis()
        while (!predicate()) {
            val spent = System.currentTimeMillis() - start
            if (spent >= timeout) break
            globalMonitor.await(timeout - spent, MILLISECONDS)
        }
        return predicate()
    }
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
