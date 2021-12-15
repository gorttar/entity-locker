package org.gorttar.concurrent.locks

import assertk.assertThat
import assertk.assertions.isLessThan
import assertk.assertions.isTrue
import org.junit.jupiter.api.Test
import java.time.Instant
import java.util.concurrent.ConcurrentLinkedQueue

internal class Event(val name: String) {
    val thread: Thread = Thread.currentThread()
    val time: Instant = Instant.now()
    override fun toString(): String = "Event $name@${thread.name}|$time"
}

private const val start = "Start"
private const val end = "End"

internal class EntityLockerTest {
    private operator fun <K1, K2, V> Map<K1, Map<K2, V>>.get(k1: K1, k2: K2): V = getValue(k1).getValue(k2)

    @Test
    fun `withLock sequential execution`() {
        // given
        val eventQueue = ConcurrentLinkedQueue<Event>()
        val locker = EntityLocker<String>()
        val protectedCode = {
            eventQueue.offer(Event(start))
            Thread.sleep(500)
            eventQueue.offer(Event(end))
        }
        val fooThread1 = Thread { locker.withLock("foo", protectedCode) }
        val fooThread2 = Thread { locker.withLock("foo", protectedCode) }
        val threads = listOf(fooThread1, fooThread2)
        // when
        threads.forEach(Thread::start)
        threads.forEach(Thread::join)
        // then
        val threadToNameToTime = eventQueue.groupBy(Event::thread).mapValues { (_, events) ->
            events.associate { it.name to it.time }
        }
        // find out which one of foo threads is first (started earlier)
        val firstFooThread: Thread
        val secondFooThread: Thread
        if (threadToNameToTime[fooThread1, start] < threadToNameToTime[fooThread2, start]) {
            firstFooThread = fooThread1
            secondFooThread = fooThread2
        } else {
            firstFooThread = fooThread2
            secondFooThread = fooThread1
        }
        // foo threads should be executed sequentially
        assertThat(threadToNameToTime[firstFooThread, end])
            .isLessThan(threadToNameToTime[secondFooThread, start])
    }

    @Test
    fun `withLock parallel execution`() {
        // given
        val eventQueue = ConcurrentLinkedQueue<Event>()
        val locker = EntityLocker<String>()
        val protectedCode = {
            eventQueue.offer(Event(start))
            Thread.sleep(500)
            eventQueue.offer(Event(end))
        }
        val fooThread = Thread { locker.withLock("foo", protectedCode) }
        val barThread = Thread { locker.withLock("bar", protectedCode) }
        val threads = listOf(fooThread, barThread)
        // when
        threads.forEach(Thread::start)
        threads.forEach(Thread::join)
        // then
        val threadToNameToTime = eventQueue.groupBy(Event::thread).mapValues { (_, events) ->
            events.associate { it.name to it.time }
        }
        // threads should be executed in parallel
        assertThat(threadToNameToTime[fooThread, start])
            .isLessThan(threadToNameToTime[barThread, end])
        assertThat(threadToNameToTime[barThread, start])
            .isLessThan(threadToNameToTime[fooThread, end])
    }

    @Test
    fun `withLock reentrant execution`() {
        val locker = EntityLocker<String>()
        var reentered = false
        var finished = false
        Thread {
            locker.withLock("foo") { locker.withLock("foo") { reentered = true } }
            finished = true
        }.apply(Thread::start).join(500)
        assertThat(reentered).isTrue()
        assertThat(finished).isTrue()
    }
}