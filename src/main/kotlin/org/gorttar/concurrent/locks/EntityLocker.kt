package org.gorttar.concurrent.locks

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
    private val globalLock = ReentrantLock()
    private val syncCondition = globalLock.newCondition()
    private val keyToLockCount = mutableMapOf<K, LockCont>()

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

    @PublishedApi
    internal fun lock(key: K): Unit = globalLock.withLock {
        while ((keyToLockCount[key]?.count ?: 0) > 0 && keyToLockCount[key]?.thread !== Thread.currentThread()) {
            syncCondition.await()
        }
        keyToLockCount[key] = (keyToLockCount[key] ?: LockCont()).inc()
    }

    @PublishedApi
    internal fun unlock(key: K): Unit = globalLock.withLock {
        if (keyToLockCount[key]?.thread === Thread.currentThread()) {
            val lockCont = keyToLockCount[key]!!.dec()
            if (lockCont.count == 0) {
                keyToLockCount.remove(key)
                syncCondition.signalAll()
            }
        }
    }
}

private class LockCont {
    val thread: Thread = Thread.currentThread()
    var count = 0
        private set

    fun inc() = apply { count++ }
    fun dec() = apply { count-- }
}
