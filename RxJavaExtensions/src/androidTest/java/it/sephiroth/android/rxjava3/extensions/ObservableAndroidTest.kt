package it.sephiroth.android.rxjava3.extensions

import android.os.SystemClock
import androidx.test.ext.junit.runners.AndroidJUnit4
import androidx.test.filters.LargeTest
import io.reactivex.rxjava3.core.Observable
import io.reactivex.rxjava3.schedulers.Schedulers
import io.reactivex.rxjava3.subjects.BehaviorSubject
import io.reactivex.rxjava3.subjects.PublishSubject
import it.sephiroth.android.rxjava3.extensions.completable.delay
import it.sephiroth.android.rxjava3.extensions.observable.ObservableUtils
import it.sephiroth.android.rxjava3.extensions.observable.autoRefresh
import it.sephiroth.android.rxjava3.extensions.observable.autoSubscribe
import it.sephiroth.android.rxjava3.extensions.observable.debug
import it.sephiroth.android.rxjava3.extensions.observable.debugWithThread
import it.sephiroth.android.rxjava3.extensions.observable.doAfterFirst
import it.sephiroth.android.rxjava3.extensions.observable.doAfterNth
import it.sephiroth.android.rxjava3.extensions.observable.doOnFirst
import it.sephiroth.android.rxjava3.extensions.observable.doOnNth
import it.sephiroth.android.rxjava3.extensions.observable.firstInList
import it.sephiroth.android.rxjava3.extensions.observable.mapList
import it.sephiroth.android.rxjava3.extensions.observable.mapNotNull
import it.sephiroth.android.rxjava3.extensions.observable.muteUntil
import it.sephiroth.android.rxjava3.extensions.observable.observeMain
import it.sephiroth.android.rxjava3.extensions.observable.refreshEvery
import it.sephiroth.android.rxjava3.extensions.observable.retry
import it.sephiroth.android.rxjava3.extensions.observable.retryWhen
import it.sephiroth.android.rxjava3.extensions.observable.toSingle
import it.sephiroth.android.rxjava3.extensions.observers.AutoDisposableObserver
import it.sephiroth.android.rxjava3.extensions.operators.ObservableTransformers
import org.junit.Assert
import org.junit.BeforeClass
import org.junit.Test
import org.junit.runner.RunWith
import java.util.Date
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong


/**
 * RxJavaExtensions
 *
 * @author Alessandro Crugnola on 02.03.21 - 13:47
 */
@RunWith(AndroidJUnit4::class)
@LargeTest
class ObservableAndroidTest {
    @Test
    fun test001() {
        Observable.just(1, 2, 3, 4, 5).toSingle().test().await().assertComplete().assertResult(1)
    }

    @Test
    fun test002() {
        Observable.just(listOf(1, 2, 3, 4, 5)).firstInList().test().await().assertComplete()
            .assertResult(1)
        Observable.just(emptyList<Int>()).firstInList().test().await().assertComplete()
            .assertNoValues()
    }

    @Test
    fun test002c() {
        Observable.just(listOf(1, 2, 3, 4, 5))
            .firstInList { it % 2 == 0 }
            .test().await().assertComplete().assertValue(2)

        Observable.just(emptyList<Int>())
            .firstInList { it % 2 == 0 }
            .test().await().assertComplete().assertNoValues()

        Observable.just(listOf(0, 1, 2))
            .firstInList { it % 2 == 1 }
            .test().await().assertComplete().assertValue(1)
    }

    @Test
    fun test003() {
        val firstLatch = CountDownLatch(1)
        val afterFirstLatch = CountDownLatch(4)
        val latch = CountDownLatch(8)
        val d = Observable
            .just(1, 2, 3, 4, 5).autoSubscribe {
                doOnStart { latch.countDown() }
                doOnNext { latch.countDown() }
                doOnComplete { latch.countDown() }
                doOnFinish { latch.countDown() }
                doOnFirst { firstLatch.countDown() }
                doAfterFirst { afterFirstLatch.countDown() }
            }

        latch.await()
        firstLatch.await()
        afterFirstLatch.await()

        Assert.assertTrue(d.isDisposed)
    }

    @Test
    fun test004() {
        var now = SystemClock.elapsedRealtime()
        val counter = AtomicInteger(0)
        val latch = CountDownLatch(1)
        val result = mutableListOf<Int>()
        Observable
            .just(1)
            .refreshEvery(50, TimeUnit.MILLISECONDS)
            .autoSubscribe {
                doOnStart {
                    now = SystemClock.elapsedRealtime()
                }
                doOnNext {
                    println("onNext($it)")
                    if (counter.incrementAndGet() > 5) {
                        dispose()
                        latch.countDown()
                    } else {
                        result.add(it)
                    }
                }

            }

        latch.await()
        Assert.assertEquals(6, counter.get())
        Assert.assertEquals(listOf(1, 1, 1, 1, 1), result)

        val totalTime = SystemClock.elapsedRealtime() - now
        println("totalTime: $totalTime")
        Assert.assertTrue("totalTime = $totalTime should be > 250", totalTime >= 250)
    }

    @Test
    fun test005() {
        val counter = AtomicInteger(0)
        val publisher = PublishSubject.create<Boolean>()

        val o = Observable
            .just(1)
            .subscribeOn(Schedulers.newThread())
            .autoRefresh(publisher)
            .doOnNext { counter.incrementAndGet() }
            .test()


        Thread.sleep(10)
        publisher.onNext(true)

        Thread.sleep(10)
        publisher.onNext(false)

        Thread.sleep(10)
        publisher.onNext(true)

        o.awaitCount(2)

        Assert.assertEquals(2, counter.get())
    }

    @Test
    fun test006() {
        val retryCount = AtomicInteger(0)
        val errorCount = AtomicInteger(0)
        val counter = AtomicInteger(0)
        var count = 0

        Observable.create<Int> { emitter ->
            while (true) {
                count += 1
                if (count % 2 == 0) throw IllegalStateException("Illegal even number")
                emitter.onNext(count)
                if (count >= 10) emitter.onComplete()
            }
        }.subscribeOn(Schedulers.newThread())
            .retry(
                predicate = {
                    println("[${System.currentTimeMillis()}] predicate running...")
                    retryCount.incrementAndGet()
                    true
                },
                delayBeforeRetry = 100,
                maxRetry = 1,
                timeUnit = TimeUnit.MILLISECONDS
            )
            .doOnNext {
                counter.set(counter.get() + it)
                println("[${System.currentTimeMillis()}] onNext: $it")
            }
            .doOnError {
                println("[${System.currentTimeMillis()}] onError: $it")
                errorCount.incrementAndGet()
            }
            .test()
            .await()

        Assert.assertEquals(2, retryCount.get())
        Assert.assertEquals(1, errorCount.get())
        Assert.assertEquals(4, counter.get())

    }

    @Test
    fun test007() {
        Observable.just(listOf(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
            .mapList { it * 2 }
            .test()
            .await().assertComplete()
            .assertValue(listOf(2, 4, 6, 8, 10, 12, 14, 16, 18, 20))
    }

    @Test
    fun test008() {
        val now = SystemClock.elapsedRealtime()

        Observable.just(1, 2, 3, 4, 5)
            .muteUntil(
                func = {
                    println("[${System.currentTimeMillis() - now}] mute until predicate")
                    SystemClock.elapsedRealtime() - now < 200
                },
                delay = 100,
                unit = TimeUnit.MILLISECONDS
            )
            .subscribeOn(Schedulers.newThread())
            .test()
            .await()
            .assertComplete()
            .assertValueCount(5)
            .assertValueAt(4, 5)

        val totalTime = SystemClock.elapsedRealtime() - now
        println("totalTime: $totalTime")

        Assert.assertTrue(totalTime > 200)
    }

    @Test
    fun test009() {
        val latch = CountDownLatch(5)
        val values = mutableListOf<Long>()
        val now = SystemClock.elapsedRealtime()
        var totalTime = now

        ObservableUtils.countDown(0, 3, 1, TimeUnit.SECONDS,
            { value ->
                println("onNext(time=" + (SystemClock.elapsedRealtime() - now) + ", value=" + value + ")")
                values.add(value)
                latch.countDown()
            },
            {
                println("onComplete(time=" + (SystemClock.elapsedRealtime() - now) + ")")
                totalTime = SystemClock.elapsedRealtime()
                latch.countDown()
            })

        latch.await()

        val timePassed = totalTime - now

        Assert.assertTrue(values.size == 4)
        Assert.assertEquals(listOf(0L, 1L, 2L, 3L), values)
        Assert.assertTrue("$timePassed should be greater than 3000", timePassed >= 3000)
        Assert.assertTrue("$timePassed should be less than 4000", timePassed < 4000)
    }

    @Test
    fun test010() {
        val latch = CountDownLatch(4)
        val values = mutableListOf<Long>()
        val now = SystemClock.elapsedRealtime()
        var totalTime = now
        ObservableUtils.countDown(3, 1, 1, TimeUnit.SECONDS,
            { value ->
                println("onNext(time=" + (SystemClock.elapsedRealtime() - now) + ", value=" + value + ")")
                values.add(value)
                latch.countDown()
            },
            {
                println("onComplete(time=" + (SystemClock.elapsedRealtime() - now) + ")")
                totalTime = SystemClock.elapsedRealtime()
                latch.countDown()
            })

        latch.await()

        val timePassed = totalTime - now

        Assert.assertTrue(values.size == 3)
        Assert.assertEquals(listOf(3L, 2L, 1L), values)
        Assert.assertTrue("$timePassed should be greater than 4000", timePassed >= 2000)
        Assert.assertTrue("$timePassed should be less than 5000", timePassed < 3000)
    }

    @Test
    fun test011() {
        val now = SystemClock.elapsedRealtime()
        val latch = CountDownLatch(100)
        val values = mutableListOf<Long>()
        ObservableUtils.timer(
            1,
            TimeUnit.SECONDS,
            10,
            TimeUnit.MILLISECONDS,
            onTick = { step, _ ->
                values.add(step)
                latch.countDown()

            }, null
        )

        latch.await()
        val end = SystemClock.elapsedRealtime()

        Assert.assertTrue(values.size == 100)
        Assert.assertEquals(LongRange(1, 100).toList(), values)
        Assert.assertTrue(end - now >= 1000)
        Assert.assertTrue(end - now < 2000)
    }

    @Test
    fun test012() {
        Observable
            .just(1, 2, 3, 4, 5, 6, 7)
            .mapNotNull { it ->
                if (it % 2 == 0) it
                else null
            }
            .test()
            .assertValues(2, 4, 6)
            .assertComplete()

        Observable
            .just(1, 2, 3, 4, 5, 6, 7)
            .filter { it > 0 }
            .mapNotNull { it ->
                if (it % 2 == 0) it
                else null
            }
            .filter { it > 0 }
            .test()
            .assertValues(2, 4, 6)
            .assertComplete()

        Observable
            .just(1, 2, 3, 4, 5, 6)
            .mapNotNull { null }
            .test()
            .await()
            .assertComplete()
            .assertNoValues()
            .assertComplete()


        val actual = AtomicInteger()
        val max = 5
        val o = Observable.create<Int> { emitter ->
            do {
                val current = actual.getAndSet(actual.get() + 1)
                if (current < max) {
                    emitter.onNext(actual.get())
                } else if (current == max) {
                    emitter.onComplete()
                } else {
                    println("Exceeded!")
                }
            } while (actual.get() <= max && !emitter.isDisposed)
        }

        o.test().await().assertValues(1, 2, 3, 4, 5).assertComplete()

        actual.set(5)
        o.mapNotNull { it % 2 == 0 }.test().await().assertComplete().assertNoValues()


        Observable.just(1, 2, 3).mapNotNull {
            throw RuntimeException("test exception")
        }.test().assertError(RuntimeException::class.java).await()

    }

    @Test
    fun test013() {
        val currentThread = Thread.currentThread()
        val latch = CountDownLatch(1)

        val result = mutableListOf<String>()
        val o = Observable.just(1, 2)
        o.subscribeOn(Schedulers.io())
            .observeOn(Schedulers.single())
            .autoSubscribe(AutoDisposableObserver {
                doOnStart {
                    result.add("start")
                    Assert.assertEquals(currentThread, Thread.currentThread())
                }
                doOnNext {
                    result.add("next:$it")
                    Assert.assertEquals(singleThread, Thread.currentThread())
                }
                doOnComplete {
                    result.add("complete")
                    Assert.assertEquals(singleThread, Thread.currentThread())
                }
                doOnError {
                    result.add("error")
                    Assert.assertEquals(singleThread, Thread.currentThread())
                }
                doOnFinish {
                    result.add("finish")
                    Assert.assertEquals(singleThread, Thread.currentThread())
                    latch.countDown()
                }
            })

        o.test().await().assertComplete()
        latch.await()
        Assert.assertEquals(listOf("start", "next:1", "next:2", "complete", "finish"), result)
    }


    @Test
    fun test014() {
        val currentThread = Thread.currentThread()
        val latch = CountDownLatch(1)
        val result = mutableListOf<String>()
        val o = Observable.error<Int>(RuntimeException("test"))
        o.subscribeOn(Schedulers.io())
            .observeOn(Schedulers.single())
            .autoSubscribe(AutoDisposableObserver {
                doOnStart {
                    result.add("start")
                    Assert.assertEquals(currentThread, Thread.currentThread())
                }
                doOnNext {
                    result.add("next")
                    Assert.assertEquals(singleThread, Thread.currentThread())
                }
                doOnComplete {
                    result.add("complete")
                    Assert.assertEquals(singleThread, Thread.currentThread())
                }
                doOnError {
                    result.add("error")
                    Assert.assertEquals(singleThread, Thread.currentThread())
                }
                doOnFinish {
                    result.add("finish")
                    Assert.assertEquals(singleThread, Thread.currentThread())
                    latch.countDown()
                }
            })

        o.test().await().assertError(RuntimeException::class.java)
        latch.await()
        Assert.assertEquals(listOf("start", "error", "finish"), result)
    }

    @Test
    fun test015() {
        Observable.just(1).debug("o1").debugWithThread("o1-thread").test().await()
        Observable.error<Int>(RuntimeException("test")).debug("o2").debugWithThread("o2-thread")
            .test().assertError(RuntimeException::class.java)
    }

    @Test
    fun test016() {
        val latch = CountDownLatch(1)
        val disposable = Observable.just(1, 2).delaySubscription(1, TimeUnit.SECONDS).debug("o1")
            .debugWithThread("o1-thread").autoSubscribe { }

        delay(16, TimeUnit.MILLISECONDS) {
            disposable.dispose()
            latch.countDown()
        }

        latch.await()
        Assert.assertTrue(disposable.isDisposed)
    }

    @Test
    fun test017() {
        val source = BehaviorSubject.createDefault(true)

        val latch = CountDownLatch(1)
        val now = System.currentTimeMillis()
        val elapsed = AtomicLong()
        val pausedSource = source.toSerialized().share()

        val o = ObservableUtils.pausedTimer(100, TimeUnit.MILLISECONDS, pausedSource)
            .doOnComplete {
                elapsed.set(System.currentTimeMillis() - now)
                latch.countDown()
            }

        delay(200, TimeUnit.MILLISECONDS) {
            println("opening gate...")
            source.onNext(false)
        }

        o.test().await()
        latch.await()

        println("elapsed = ${elapsed.get()}")

        Assert.assertTrue(elapsed.get() > 300)
    }

    @Test
    fun test018() {
        val source = BehaviorSubject.createDefault(true)
        val pausedSource = source.toSerialized().share()

        val latch = CountDownLatch(1)
        var now = SystemClock.elapsedRealtime()
        val elapsed = AtomicLong()

        val observable = ObservableUtils.pausedInterval(50, TimeUnit.MILLISECONDS, pausedSource)
            .doOnSubscribe {
                println("timer.subscribe")
                now = SystemClock.elapsedRealtime()
            }
            .doOnNext {
                elapsed.set(SystemClock.elapsedRealtime() - now)
                println("timer.next = ${elapsed.get()}")
                latch.countDown()
            }
            .doOnComplete {
                println("timer.complete = ${SystemClock.elapsedRealtime() - now}")
            }


        delay(200, TimeUnit.MILLISECONDS) {
            println("opening gate")
            source.onNext(false)
        }

        val disposable = observable.autoSubscribe { }

        latch.await()

        disposable.dispose()

        Assert.assertTrue(elapsed.get() in 201..300)
    }


    @Test
    fun test019() {
        val now = System.currentTimeMillis()
        val finalTime = AtomicLong()
        val maxAttempts = 5

        Observable.create<String> { emitter ->
            if (!emitter.isDisposed) {
                val delta = System.currentTimeMillis() - now
                println("[${Date()}] emitting the exception. delta = $delta")
                if (delta >= TimeUnit.SECONDS.toMillis(maxAttempts.toLong() / 2)) {
                    emitter.onNext("Emitting next Value!")
                    emitter.onComplete()
                } else {
                    emitter.tryOnError(IllegalStateException("Error emitting [$delta]"))
                }
            }
        }.retryWhen(maxAttempts) { _, retryCount ->
            println("[${Date()}] predicate[$retryCount - $maxAttempts]")
            TimeUnit.SECONDS.toMillis(1) // retry every 1 second
        }.subscribeOn(Schedulers.single())
            .doOnNext {
                println("[${Date()}] onNext")
            }.doOnComplete {
                println("[${Date()}] onComplete")
                finalTime.set(System.currentTimeMillis() - now)
            }.doOnError {
                println("[${Date()}] onError(${it.javaClass})")
            }
            .test()
            .await()
            .assertComplete()

        Assert.assertTrue(
            "final time must be >= ${maxAttempts / 2} seconds but it was ${finalTime.get()}",
            finalTime.get() >= TimeUnit.SECONDS.toMillis(maxAttempts.toLong() / 2)
        )
        Assert.assertTrue(
            "final time must be < ${(maxAttempts / 2) + 1} seconds but it was ${finalTime.get()}",
            finalTime.get() < TimeUnit.SECONDS.toMillis(maxAttempts.toLong() / 2 + 1)
        )
    }


    @Test
    fun test020() {
        val now = System.currentTimeMillis()
        val finalTime = AtomicLong()
        val maxAttempts = 10

        Observable.create<String> { emitter ->
            if (!emitter.isDisposed) {
                println("[${Date()}] emitting the exception")
                emitter.tryOnError(IllegalStateException("Error"))
            }
        }.retryWhen(maxAttempts) { throwable, retryCount ->
            println("[${Date()}] predicate[$retryCount -- $maxAttempts] (throwable: ${throwable.javaClass})")
            (retryCount * 100).toLong()
        }.subscribeOn(Schedulers.single())
            .doOnNext {
                println("[${Date()}] onNext")
            }.doOnComplete {
                println("[${Date()}] onComplete")
                finalTime.set(System.currentTimeMillis() - now)
            }.doOnError {
                println("[${Date()}] onError")
                finalTime.set(System.currentTimeMillis() - now)
            }
            .test()
            .await()
            .assertError(RetryException::class.java)
    }

    @Test
    fun test021() {
        val now = System.currentTimeMillis()
        val finalTime = AtomicLong()

        val first = AtomicLong()
        val afterFirst = AtomicLong()
        val afterSecond = AtomicLong()
        val second = AtomicLong()
        val fifth = AtomicLong()
        val fourth = AtomicLong()
        val nextCount = AtomicLong()

        Observable.just(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
            .compose(ObservableTransformers.doOnFirst {
                first.incrementAndGet()
            })
            .compose(ObservableTransformers.doAfterFirst {
                afterFirst.incrementAndGet()
            })
            .compose(ObservableTransformers.doOnNth(2) {
                second.incrementAndGet()
            })
            .compose(ObservableTransformers.doOnNth(5) {
                fifth.incrementAndGet()
            })
            .compose(ObservableTransformers.doOnNth(4) {
                fourth.incrementAndGet()
            })
            .compose(ObservableTransformers.doAfterNth(2) {
                afterSecond.incrementAndGet()
            })
            .doOnNext {
                nextCount.incrementAndGet()
            }.doOnComplete {
                finalTime.set(System.currentTimeMillis() - now)
            }.doOnError {
                finalTime.set(System.currentTimeMillis() - now)
            }
            .test()
            .await()
            .assertComplete()

        Assert.assertEquals(10, nextCount.get())
        Assert.assertEquals(1, first.get())
        Assert.assertEquals(9, afterFirst.get())
        Assert.assertEquals(1, second.get())
        Assert.assertEquals(1, fifth.get())
        Assert.assertEquals(1, fourth.get())
        Assert.assertEquals(7, afterSecond.get())
    }

    @Test
    fun test022() {
        val now = System.currentTimeMillis()
        val finalTime = AtomicLong()

        val first = AtomicLong()
        val afterFirst = AtomicLong()
        val nextCount = AtomicLong()

        Observable.just(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
            .doOnFirst {
                first.incrementAndGet()
            }
            .doAfterFirst {
                afterFirst.incrementAndGet()
            }
            .doOnNext {
                nextCount.incrementAndGet()
            }.doOnComplete {
                finalTime.set(System.currentTimeMillis() - now)
            }.doOnError {
                finalTime.set(System.currentTimeMillis() - now)
            }
            .test()
            .await()
            .assertComplete()

        Assert.assertEquals(10, nextCount.get())
        Assert.assertEquals(1, first.get())
        Assert.assertEquals(9, afterFirst.get())
    }

    @Test
    fun test023() {
        val now = System.currentTimeMillis()
        val finalTime = AtomicLong()

        val first = AtomicLong()
        val afterFirst = AtomicLong()
        val afterSecond = AtomicLong()
        val nextCount = AtomicLong()
        val fifth = AtomicLong()

        Observable.just(1, 2, 3)
            .doOnFirst {
                first.incrementAndGet()
            }
            .doAfterFirst {
                afterFirst.incrementAndGet()
            }
            .doOnNth(5) {
                fifth.incrementAndGet()
            }
            .doAfterNth(1) {
                afterSecond.incrementAndGet()
            }
            .doOnNext {
                nextCount.incrementAndGet()
            }.doOnComplete {
                finalTime.set(System.currentTimeMillis() - now)
            }.doOnError {
                finalTime.set(System.currentTimeMillis() - now)
            }
            .test()
            .await()
            .assertComplete()

        Assert.assertEquals(3, nextCount.get())
        Assert.assertEquals(1, first.get())
        Assert.assertEquals(2, afterFirst.get())
        Assert.assertEquals(1, afterSecond.get())
        Assert.assertEquals(0, fifth.get())
    }

    @Test
    fun test024() {
        val latch = CountDownLatch(1)
        val disposable = Observable.just(1, 2, 3, 4, 5)
            .observeMain()
            .autoSubscribe {
                doOnComplete {
                    println("onComplete")
                    latch.countDown()
                }

                doOnStart {
                    println("onStart")
                }

                doOnFinish {
                    println("onFinish")
                }

                doOnNext {
                    println("onNext: $it")
                }

                doOnDispose {
                    println("onDispose")
                }

                doOnError {
                    println("onError: $it")
                }
            }

        latch.await()
        disposable.dispose()
        Assert.assertTrue(disposable.isDisposed)
    }

    companion object {
        private lateinit var ioThread: Thread
        private lateinit var singleThread: Thread

        @JvmStatic
        @BeforeClass
        fun before() {
            val latch = CountDownLatch(2)

            Schedulers.single().scheduleDirect {
                singleThread = Thread.currentThread()
                latch.countDown()
            }

            Schedulers.io().scheduleDirect {
                ioThread = Thread.currentThread()
                latch.countDown()
            }

            latch.await()

        }
    }
}
