package it.sephiroth.android.rxjava3.extensions

import androidx.test.ext.junit.runners.AndroidJUnit4
import androidx.test.filters.LargeTest
import io.reactivex.rxjava3.android.schedulers.AndroidSchedulers
import io.reactivex.rxjava3.core.BackpressureStrategy
import io.reactivex.rxjava3.core.Flowable
import io.reactivex.rxjava3.core.Observable
import io.reactivex.rxjava3.schedulers.Schedulers
import io.reactivex.rxjava3.subjects.BehaviorSubject
import it.sephiroth.android.rxjava3.extensions.completable.delay
import it.sephiroth.android.rxjava3.extensions.flowable.*
import it.sephiroth.android.rxjava3.extensions.observable.doAfterFirst
import it.sephiroth.android.rxjava3.extensions.observable.doOnFirst
import it.sephiroth.android.rxjava3.extensions.observers.AutoDisposableSubscriber
import it.sephiroth.android.rxjava3.extensions.operators.FlowableTransformers
import it.sephiroth.android.rxjava3.extensions.operators.ObservableTransformers
import org.junit.Assert
import org.junit.BeforeClass
import org.junit.Test
import org.junit.runner.RunWith
import java.util.*
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong


/**
 * RxJavaExtensions
 *
 * @author Alessandro Crugnola on 01.03.21 - 10:13
 */
@RunWith(AndroidJUnit4::class)
@LargeTest
class FlowableAndroidTest {

    @Test
    fun test001() {
        val latch = CountDownLatch(8)
        val d = Flowable
            .just(1, 2, 3, 4, 5)
            .observeMain()
            .autoSubscribe {
                doOnStart {
                    println("doOnStart")
                    latch.countDown()
                }
                doOnNext {
                    println("doOnNext: $it")
                    latch.countDown()
                }
                doOnComplete {
                    println("doOnComplete")
                    latch.countDown()
                }
                doOnFinish {
                    println("doOnFinish")
                    latch.countDown()
                }
            }
        latch.await(1, TimeUnit.SECONDS)
        Assert.assertTrue(d.isDisposed)
    }

    @Test
    fun test002() {
        val currentThread = Thread.currentThread()
        val latch = CountDownLatch(1)

        val result = mutableListOf<String>()
        val flowable = Flowable.just(1, 2)
        flowable.subscribeOn(Schedulers.io())
            .observeOn(Schedulers.single())
            .autoSubscribe(AutoDisposableSubscriber {
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

        flowable.test().await().assertComplete()
        latch.await()
        Assert.assertEquals(listOf("start", "next:1", "next:2", "complete", "finish"), result)
    }

    @Test
    fun test003() {
        val currentThread = Thread.currentThread()
        val latch = CountDownLatch(1)
        val result = mutableListOf<String>()
        val flowable = Flowable.error<Int>(RuntimeException("test"))
        flowable.subscribeOn(Schedulers.io())
            .observeOn(Schedulers.single())
            .autoSubscribe(AutoDisposableSubscriber {
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

        flowable.test().await().assertError(RuntimeException::class.java)
        latch.await()
        Assert.assertEquals(listOf("start", "error", "finish"), result)
    }

    @Test
    fun test004() {
        Flowable.just(1).debug("f1").debugWithThread("f1-thread").test().await()
        Flowable.error<Int>(RuntimeException("test")).debug("f2").debugWithThread("f2-thread")
            .test().assertError(RuntimeException::class.java)
    }

    @Test
    fun test005() {
        val latch = CountDownLatch(1)
        val disposable = Flowable.just(1, 2)
            .delaySubscription(1, TimeUnit.SECONDS)
            .debug("f1").debugWithThread("f1-thread").autoSubscribe { }

        delay(16, TimeUnit.MILLISECONDS) {
            disposable.dispose()
            latch.countDown()
        }

        latch.await()
        Assert.assertTrue(disposable.isDisposed)
    }

    @Test
    fun test006() {
        val currentThread = Thread.currentThread()
        val latch = CountDownLatch(7)
        val firstLatch = CountDownLatch(1)
        val afterFirstLatch = CountDownLatch(3)
        Flowable.just(1, 2, 3, 4)
            .subscribeOn(Schedulers.io())
            .observeMain()
            .autoSubscribe {
                doOnFirst {
                    Assert.assertEquals(mainThread, Thread.currentThread())
                    firstLatch.countDown()
                }

                doAfterFirst {
                    Assert.assertEquals(mainThread, Thread.currentThread())
                    afterFirstLatch.countDown()
                }

                doOnStart {
                    Assert.assertEquals(currentThread, Thread.currentThread())
                    latch.countDown()
                }
                doOnError {
                    Assert.assertEquals(mainThread, Thread.currentThread())
                    latch.countDown()
                }
                doOnNext {
                    latch.countDown()
                    Assert.assertEquals(mainThread, Thread.currentThread())
                }
                doOnComplete {
                    latch.countDown()
                    Assert.assertEquals(mainThread, Thread.currentThread())
                }
                doOnFinish {
                    latch.countDown()
                    Assert.assertEquals(mainThread, Thread.currentThread())
                }
            }

        latch.await(1, TimeUnit.SECONDS)
        firstLatch.await(1, TimeUnit.SECONDS)
        afterFirstLatch.await(1, TimeUnit.SECONDS)
    }

    @Test
    fun test007() {
        val count = AtomicInteger(0)
        Flowable.create<Int>({ emitter ->
            emitter.onNext(count.getAndIncrement())
            Thread.sleep(50)
            emitter.onNext(count.getAndIncrement())
            emitter.onNext(count.getAndIncrement())
            Thread.sleep(250)
            emitter.onNext(count.getAndIncrement())
            Thread.sleep(50)
            emitter.onNext(count.getAndIncrement())
            Thread.sleep(250)
            emitter.onNext(count.getAndIncrement())
            emitter.onComplete()

        }, BackpressureStrategy.BUFFER)
            .subscribeOn(Schedulers.computation())
            .skipBetween(200, TimeUnit.MILLISECONDS, true)
            .test()
            .await()
            .assertComplete()
            .assertValues(0, 3, 5)
    }

    @Test
    fun test008() {
        val count = AtomicInteger(0)
        Flowable.create<Int>({ emitter ->
            emitter.onNext(count.getAndIncrement())
            Thread.sleep(50)
            emitter.onNext(count.getAndIncrement())
            emitter.onNext(count.getAndIncrement())
            Thread.sleep(250)
            emitter.onNext(count.getAndIncrement())
            Thread.sleep(50)
            emitter.onNext(count.getAndIncrement())
            Thread.sleep(250)
            emitter.onNext(count.getAndIncrement())
            emitter.onComplete()

        }, BackpressureStrategy.BUFFER)
            .subscribeOn(Schedulers.computation())
            .skipBetween(200, TimeUnit.MILLISECONDS, false)
            .test()
            .await()
            .assertComplete()
            .assertValues(3, 5)
    }

    @Test
    fun test009() {
        val subject = BehaviorSubject.create<ITestEvent>()
        val valueCount = CountDownLatch(5)
        val values = mutableListOf<ITestEvent>()

        val subscriber = subject.toFlowable(BackpressureStrategy.BUFFER)
            .pingPong(TestEvent01::class.java, TestEvent03::class.java)
            .doOnNext {
                println("onNext: $it")
                valueCount.countDown()
                values.add(it)
            }.test()

        subject.onNext(TestEvent01)
        subject.onNext(TestEvent02)
        subject.onNext(TestEvent03)
        subject.onNext(TestEvent02)
        subject.onNext(TestEvent01)
        subject.onNext(TestEvent01)
        subject.onNext(TestEvent01)
        subject.onNext(TestEvent03)
        subject.onNext(TestEvent03)
        subject.onNext(TestEvent02)
        subject.onNext(TestEvent01)
        subject.onComplete()

        subscriber.await()

        valueCount.await()

        Assert.assertEquals(
            listOf(
                TestEvent01,
                TestEvent03,
                TestEvent01,
                TestEvent03,
                TestEvent01
            ), values
        )
    }

    @Test
    fun test010() {
        Flowable.just(listOf(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
            .mapList { it * 2 }
            .test()
            .await().assertComplete()
            .assertValue(listOf(2, 4, 6, 8, 10, 12, 14, 16, 18, 20))
    }

    @Test
    fun test011() {
        Flowable.just(1, 2, 3, 4, 5, 6, 7)
            .mapNotNull { it ->
                if (it % 2 == 0) it
                else null
            }
            .test()
            .assertValues(2, 4, 6)
            .assertComplete()

        Flowable.just(1, 2, 3, 4, 5, 6, 7)
            .filter { it > 0 }
            .mapNotNull { it ->
                if (it % 2 == 0) it
                else null
            }
            .filter { it > 0 }
            .test()
            .assertValues(2, 4, 6)
            .assertComplete()

        Flowable.just(1, 2, 3, 4, 5, 6)
            .mapNotNull { null }
            .test()
            .await()
            .assertComplete()
            .assertNoValues()
            .assertComplete()


        val actual = AtomicInteger()
        val max = 5
        val flowable = Flowable.create<Int>({ emitter ->
            do {
                val current = actual.getAndSet(actual.get() + 1)
                if (current < max) {
                    emitter.onNext(actual.get())
                } else if (current == max) {
                    emitter.onComplete()
                } else {
                    println("Exceeded!")
                }
            } while (actual.get() <= max && !emitter.isCancelled)
        }, BackpressureStrategy.BUFFER)

        flowable.test().await().assertValues(1, 2, 3, 4, 5).assertComplete()

        actual.set(5)
        flowable.mapNotNull { it % 2 == 0 }.test().await().assertComplete().assertNoValues()


        Flowable.just(1, 2, 3).mapNotNull { throw RuntimeException("test exception") }.test()
            .assertError(RuntimeException::class.java).await()

    }

    @Test
    fun test012() {
        Flowable.just(1, 2, 3, 4, 5).toSingle().test().await().assertComplete().assertResult(1)
    }

    @Test
    fun test013() {
        Flowable.just(listOf(1, 2, 3, 4, 5)).firstInList().test().await().assertComplete()
            .assertResult(1)
        Flowable.just(emptyList<Int>()).firstInList().test().await().assertComplete()
            .assertNoValues()
    }

    @Test
    fun test015() {
        Flowable.just(listOf(1, 2, 3, 4, 5))
            .firstInList { it % 2 == 0 }
            .test().await().assertComplete().assertValue(2)

        Flowable.just(emptyList<Int>())
            .firstInList { it % 2 == 0 }
            .test().await().assertComplete().assertNoValues()
    }

    @Test
    fun test016() {
        val now = System.currentTimeMillis()
        val finalTime = AtomicLong()
        val maxAttempts = 5

        Flowable.create<String>({ emitter ->
            if (!emitter.isCancelled) {
                val delta = System.currentTimeMillis() - now
                println("[${Date()}] emitting the exception. delta = $delta")
                if (delta >= TimeUnit.SECONDS.toMillis(maxAttempts.toLong() / 2)) {
                    emitter.onNext("Emitting next Value!")
                    emitter.onComplete()
                } else {
                    emitter.tryOnError(IllegalStateException("Error emitting [$delta]"))
                }
            }
        }, BackpressureStrategy.BUFFER).retryWhen(maxAttempts) { _, retryCount ->
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
    fun test017() {
        val now = System.currentTimeMillis()
        val finalTime = AtomicLong()
        val maxAttempts = 10

        Flowable.create<String>({ emitter ->
            if (!emitter.isCancelled) {
                println("[${Date()}] emitting the exception")
                emitter.tryOnError(IllegalStateException("Error"))
            }
        }, BackpressureStrategy.BUFFER).retryWhen(maxAttempts) { throwable, retryCount ->
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
    fun test018() {
        val now = System.currentTimeMillis()
        val finalTime = AtomicLong()

        val first = AtomicLong()
        val afterFirst = AtomicLong()
        val afterSecond = AtomicLong()
        val second = AtomicLong()
        val fifth = AtomicLong()
        val fourth = AtomicLong()
        val nextCount = AtomicLong()

        Flowable.just(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
            .compose(FlowableTransformers.doOnFirst {
                first.incrementAndGet()
            })
            .compose(FlowableTransformers.doAfterFirst {
                afterFirst.incrementAndGet()
            })
            .compose(FlowableTransformers.doOnNth(2) {
                second.incrementAndGet()
            })
            .compose(FlowableTransformers.doOnNth(5) {
                fifth.incrementAndGet()
            })
            .compose(FlowableTransformers.doOnNth(4) {
                fourth.incrementAndGet()
            })
            .compose(FlowableTransformers.doAfterNth(2) {
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
    fun test019() {
        val now = System.currentTimeMillis()
        val finalTime = AtomicLong()

        val first = AtomicLong()
        val afterFirst = AtomicLong()
        val nextCount = AtomicLong()

        Flowable.just(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
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

    companion object {
        private lateinit var mainThread: Thread
        private lateinit var ioThread: Thread
        private lateinit var singleThread: Thread

        @JvmStatic
        @BeforeClass
        fun before() {
            val latch = CountDownLatch(3)

            Schedulers.single().scheduleDirect {
                singleThread = Thread.currentThread()
                latch.countDown()
            }

            Schedulers.io().scheduleDirect {
                ioThread = Thread.currentThread()
                latch.countDown()
            }

            AndroidSchedulers.mainThread().scheduleDirect {
                mainThread = Thread.currentThread()
                latch.countDown()
            }

            latch.await()

        }
    }

    interface ITestEvent
    object TestEvent01 : ITestEvent
    object TestEvent02 : ITestEvent
    object TestEvent03 : ITestEvent
}
