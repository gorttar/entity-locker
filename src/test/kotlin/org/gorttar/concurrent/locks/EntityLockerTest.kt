package org.gorttar.concurrent.locks

import assertk.assertThat
import assertk.assertions.*
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
private const val tickDurationMs = 100L
private const val twoTicksDurationMs = 2 * tickDurationMs
private const val threeTicksDurationMs = 3 * tickDurationMs

internal class EntityLockerTest {
    private val locker = EntityLocker<String>()
    private val eventQueue = ConcurrentLinkedQueue<Event>()
    private val protectedCode: () -> Unit = {
        eventQueue.offer(Event(start))
        Thread.sleep(twoTicksDurationMs)
        eventQueue.offer(Event(end))
    }
    private val fooThread = Thread { locker.withLock("foo", protectedCode) }

    private var reentered = false
    private var finished = false
    private var result1: TryLockResult<*>? = null
    private var result2: TryLockResult<*>? = null

    @Test
    fun `withLock sequential execution`() {
        // given
        val fooThread2 = Thread { locker.withLock("foo", protectedCode) }
        // when
        listOf(fooThread, fooThread2).onEach(Thread::start).forEach(Thread::join)
        // then
        val threadToNameToTime = threadToNameToTime()
        // find out which one of foo threads is first (started earlier)
        val firstFooThread: Thread
        val secondFooThread: Thread
        if (threadToNameToTime[fooThread, start] < threadToNameToTime[fooThread2, start]) {
            firstFooThread = fooThread
            secondFooThread = fooThread2
        } else {
            firstFooThread = fooThread2
            secondFooThread = fooThread
        }
        // foo threads should be executed sequentially
        assertThat(threadToNameToTime[firstFooThread, end])
            .isLessThan(threadToNameToTime[secondFooThread, start])
    }

    @Test
    fun `withLock parallel execution`() {
        // given
        val barThread = Thread { locker.withLock("bar", protectedCode) }
        // when
        listOf(fooThread, barThread).onEach(Thread::start).forEach(Thread::join)
        // then
        val threadToNameToTime = threadToNameToTime()
        // threads should be executed in parallel
        assertThat(threadToNameToTime[fooThread, start])
            .isLessThan(threadToNameToTime[barThread, end])
        assertThat(threadToNameToTime[barThread, start])
            .isLessThan(threadToNameToTime[fooThread, end])
    }

    @Test
    fun `withLock reentrant execution`() {
        // when
        Thread {
            locker.withLock("foo") { locker.withLock("foo") { reentered = true } }
            finished = true
        }.apply(Thread::start).join(twoTicksDurationMs)
        // then
        assertThat(reentered).isTrue()
        assertThat(finished).isTrue()
    }

    @Test
    fun `withTryLock sequential execution`() {
        // given
        val fooThread1 = Thread { result1 = locker.withTryLock("foo", threeTicksDurationMs, protectedCode) }
        val fooThread2 = Thread { result2 = locker.withTryLock("foo", threeTicksDurationMs, protectedCode) }
        // when
        listOf(fooThread1, fooThread2).onEach(Thread::start).forEach(Thread::join)
        // then
        assertThat(result1).isEqualTo(Success(Unit))
        assertThat(result2).isEqualTo(Success(Unit))
        val threadToNameToTime = threadToNameToTime()
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
    fun `withTryLock sequential execution timeout`() {
        // given
        val fooThread1 = Thread {
            result1 = locker.withTryLock("foo", twoTicksDurationMs) {
                eventQueue.offer(Event(start))
                Thread.sleep(threeTicksDurationMs)
                eventQueue.offer(Event(end))
            }
        }
        val fooThread2 = Thread { result2 = locker.withTryLock("foo", tickDurationMs, protectedCode) }
        // when
        listOf(fooThread1, fooThread2).onEach {
            it.start()
            Thread.sleep(tickDurationMs)
        }.forEach(Thread::join)
        // then
        assertThat(result1).isEqualTo(Success(Unit))
        assertThat(result2).isEqualTo(TimeoutExceeded)
        val threadToNameToTime = threadToNameToTime()
        assertThat(threadToNameToTime[fooThread1, start]).isNotNull()
        assertThat(threadToNameToTime[fooThread1, end]).isNotNull()
        assertThat(threadToNameToTime[fooThread2]).isNull()
    }

    @Test
    fun `withTryLock parallel execution`() {
        // given
        val fooThread = Thread { result1 = locker.withTryLock("foo", threeTicksDurationMs, protectedCode) }
        val barThread = Thread { result2 = locker.withTryLock("bar", threeTicksDurationMs, protectedCode) }
        // when
        listOf(fooThread, barThread).onEach(Thread::start).forEach(Thread::join)
        // then
        assertThat(result1).isEqualTo(Success(Unit))
        assertThat(result2).isEqualTo(Success(Unit))
        val threadToNameToTime = threadToNameToTime()
        // threads should be executed in parallel
        assertThat(threadToNameToTime[fooThread, start])
            .isLessThan(threadToNameToTime[barThread, end])
        assertThat(threadToNameToTime[barThread, start])
            .isLessThan(threadToNameToTime[fooThread, end])
    }

    @Test
    fun `withTryLock reentrant execution`() {
        //when
        Thread {
            result1 = locker.withTryLock("foo", threeTicksDurationMs) {
                result2 = locker.withTryLock("foo", threeTicksDurationMs) {
                    reentered = true
                }
            }
            finished = true
        }.apply(Thread::start).join(twoTicksDurationMs)
        // then
        assertThat(result1).isEqualTo(Success(Unit))
        assertThat(result2).isEqualTo(Success(Unit))
        assertThat(reentered).isTrue()
        assertThat(finished).isTrue()
    }

    @Test
    fun `withGlobalLock sequential execution local before global`() {
        // given
        val globalThread = Thread { locker.withGlobalLock(protectedCode) }
        // when
        listOf(fooThread, globalThread).onEach {
            it.start()
            Thread.sleep(tickDurationMs)
        }.forEach(Thread::join)
        // then
        val threadToNameToTime = threadToNameToTime()
        // fooThread1 should be executed before globalThread
        assertThat(threadToNameToTime[fooThread, end])
            .isLessThan(threadToNameToTime[globalThread, start])
    }

    @Test
    fun `withGlobalLock sequential execution global before local`() {
        // given
        val globalThread = Thread { locker.withGlobalLock(protectedCode) }
        // when
        listOf(globalThread, fooThread).onEach {
            it.start()
            Thread.sleep(tickDurationMs)
        }.forEach(Thread::join)
        // then
        val threadToNameToTime = threadToNameToTime()
        // globalThread should be executed before fooThread1
        assertThat(threadToNameToTime[globalThread, end])
            .isLessThan(threadToNameToTime[fooThread, start])
    }

    @Test
    fun `withGlobalLock reentrant execution`() {
        // when
        Thread {
            locker.withGlobalLock { locker.withGlobalLock { reentered = true } }
            finished = true
        }.apply(Thread::start).join(twoTicksDurationMs)
        // then
        assertThat(reentered).isTrue()
        assertThat(finished).isTrue()
    }

    private operator fun <K1, K2, V> Map<K1, Map<K2, V>>.get(k1: K1, k2: K2): V = getValue(k1).getValue(k2)
    private fun threadToNameToTime() = eventQueue.groupBy(Event::thread).mapValues { (_, events) ->
        events.associate { it.name to it.time }
    }
}
