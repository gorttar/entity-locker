package org.gorttar.concurrent.locks

/**
 * Utility class that provides synchronization mechanism similar to row-level DB locking.
 * The class is supposed to be used by the components that are responsible for managing storage
 * and caching of different type of entities in the application.
 * [EntityLocker] itself does not deal with the entities, only with the IDs (primary keys) of the entities.
 * [K] is the type of entity ID
 */
class EntityLocker<K : Any> {
    /**
     * executes [protectedCode] exclusively accessing entity determined by [key]
     * ensuring that at most one [Thread] executes protected code on that entity
     * If there’s a concurrent request to lock the same entity, the other thread
     * should wait until the entity becomes available.
     * Concurrent execution of protected code on different entities is allowed.
     * Reentrant invocation is allowed.
     */
    inline fun <T> runLocking(key: K, protectedCode: () -> T): T {
        TODO()
    }
}