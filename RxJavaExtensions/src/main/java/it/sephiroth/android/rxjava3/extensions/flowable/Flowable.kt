/*
 * MIT License
 *
 * Copyright (c) 2021 Alessandro Crugnola
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

@file:Suppress("unused")

package it.sephiroth.android.rxjava3.extensions.flowable

import android.annotation.SuppressLint
import android.util.Log
import io.reactivex.rxjava3.android.schedulers.AndroidSchedulers
import io.reactivex.rxjava3.annotations.CheckReturnValue
import io.reactivex.rxjava3.annotations.SchedulerSupport
import io.reactivex.rxjava3.core.Flowable
import io.reactivex.rxjava3.core.Maybe
import io.reactivex.rxjava3.core.Single
import io.reactivex.rxjava3.functions.BiFunction
import io.reactivex.rxjava3.functions.Function
import io.reactivex.rxjava3.plugins.RxJavaPlugins
import it.sephiroth.android.rxjava3.extensions.MaxRetryCountExceededException
import it.sephiroth.android.rxjava3.extensions.RetryException
import it.sephiroth.android.rxjava3.extensions.observable.autoSubscribe
import it.sephiroth.android.rxjava3.extensions.observable.doAfterFirst
import it.sephiroth.android.rxjava3.extensions.observable.doOnFirst
import it.sephiroth.android.rxjava3.extensions.observers.AutoDisposableObserver
import it.sephiroth.android.rxjava3.extensions.observers.AutoDisposableSubscriber
import it.sephiroth.android.rxjava3.extensions.operators.FlowableMapNotNull
import it.sephiroth.android.rxjava3.extensions.operators.FlowableTransformers
import it.sephiroth.android.rxjava3.extensions.single.firstInList
import java.util.Objects
import java.util.concurrent.TimeUnit
import java.util.function.Predicate

/**
 * RxJavaExtensions
 *
 * @author Alessandro Crugnola on 06.01.21 - 13:31
 */

/**
 * Subscribe the source using an instance of the [AutoDisposableObserver].
 * The source will be disposed when a complete or error event is received.
 */
fun <T> Flowable<T>.autoSubscribe(observer: AutoDisposableSubscriber<T>): AutoDisposableSubscriber<T> where T : Any {
    var flowable = this
    observer._doOnFirst?.let {
        flowable = flowable.doOnFirst(it)
    }
    observer._doAfterFirst?.let {
        flowable = flowable.doAfterFirst(it)
    }
    return flowable.subscribeWith(observer)
}

/**
 * @see [autoSubscribe]
 */
fun <T> Flowable<T>.autoSubscribe(
    builder: (AutoDisposableSubscriber<T>.() -> Unit)
): AutoDisposableSubscriber<T> where T : Any {
    return this.autoSubscribe(AutoDisposableSubscriber(builder))
}

@SuppressLint("LogNotTimber")
fun <T> Flowable<T>.debug(tag: String): Flowable<T> where T : Any {
    return this
        .doOnNext { Log.v(tag, "onNext($it)") }
        .doOnError { Log.e(tag, "onError(${it.message})") }
        .doOnSubscribe { Log.v(tag, "onSubscribe()") }
        .doOnComplete { Log.v(tag, "onComplete()") }
        .doOnCancel { Log.w(tag, "onCancel()") }
        .doOnRequest { Log.w(tag, "onRequest()") }
        .doOnTerminate { Log.w(tag, "onTerminate()") }
}

@SuppressLint("LogNotTimber")
fun <T> Flowable<T>.debugWithThread(tag: String): Flowable<T> where T : Any {
    return this
        .doOnNext { Log.v(tag, "[${Thread.currentThread().name}] onNext($it)") }
        .doOnError { Log.e(tag, "[${Thread.currentThread().name}] onError(${it.message})") }
        .doOnSubscribe { Log.v(tag, "[${Thread.currentThread().name}] onSubscribe()") }
        .doOnComplete { Log.v(tag, "[${Thread.currentThread().name}] onComplete()") }
        .doOnCancel { Log.w(tag, "[${Thread.currentThread().name}] onCancel()") }
        .doOnRequest { Log.w(tag, "[${Thread.currentThread().name}] onRequest()") }
        .doOnTerminate { Log.w(tag, "[${Thread.currentThread().name}] onTerminate()") }
}

/**
 * alias for Flowable.observeOn(AndroidSchedulers.mainThread())
 */
fun <T> Flowable<T>.observeMain(): Flowable<T> where T : Any {
    return observeOn(AndroidSchedulers.mainThread())
}

/**
 * Returns a Flowable that skips all items emitted by the source emitter
 * until a specified interval passed between each emission..
 *
 * @param time minimum amount of time need to pass between each interactions
 * @param unit time unit for the [time] param
 * @param defaultOpened if true the very first emission of the source [Flowable] will be allowed, false otherwise
 *
 */
fun <T : Any> Flowable<T>.skipBetween(
    time: Long,
    unit: TimeUnit,
    defaultOpened: Boolean
): Flowable<T> {
    var t: Long = if (defaultOpened) 0 else System.currentTimeMillis()
    return this.filter {
        val waitTime = unit.toMillis(time)
        val elapsed = (System.currentTimeMillis() - t)
        if (elapsed > waitTime) {
            t = System.currentTimeMillis()
            true
        } else {
            false
        }
    }
}

/**
 * Returns a Flowable that filter out those objects not of the type of [cls1] and [cls2].
 * Moreover objects must alternate between cls1 and cls2, otherwise the object is skipped.
 *
 * For instance this code:
 *
 *             subject.retry()
 *             .toFlowable(BackpressureStrategy.BUFFER)
 *             .pingPong(TestEventImpl2::class.java, TestEventImpl4::class.java)
 *               .doOnNext { it ->
 *                   Log.v("FlowableTest", "onNext = $it")
 *               }
 *               .subscribe()
 *               subject.onNext(TestEventImpl1())
 *               subject.onNext(TestEventImpl2())
 *               subject.onNext(TestEventImpl2())
 *               subject.onNext(TestEventImpl3())
 *               subject.onNext(TestEventImpl4())
 *               subject.onNext(TestEventImpl4())
 *               subject.onNext(TestEventImpl1())
 *               subject.onNext(TestEventImpl2())
 *               subject.onNext(TestEventImpl3())
 *               subject.onNext(TestEventImpl4())
 *
 * It will only output the following:
 *
 *      FlowableTest: onNext = TestEventImpl2
 *      FlowableTest: onNext = TestEventImpl4
 *      FlowableTest: onNext = TestEventImpl2
 *      FlowableTest: onNext = TestEventImpl4
 */
fun <T, E, R> Flowable<T>.pingPong(
    cls1: Class<E>,
    cls2: Class<R>
): Flowable<T> where E : T, R : T, T : Any {
    var current: Class<*>? = null
    return this.doOnSubscribe {
        current = null
    }.filter { t2: T ->
        if (cls1.isInstance(t2) || cls2.isInstance(t2)) {
            val t2Class = t2::class.java
            val result = if (t2Class != cls1 && t2Class != cls2) {
                true
            } else {
                if (null == current) {
                    if (t2Class == cls2 || t2Class == cls1) {
                        current = t2Class
                        true
                    } else {
                        false
                    }
                } else {
                    if (current == t2Class) {
                        false
                    } else {
                        current = t2Class
                        true
                    }
                }
            }
            result
        } else {
            false
        }
    }
}

/**
 * Maps the elements of a list emitted by the source [Flowable]
 * @since 3.0.5
 */
@CheckReturnValue
@SchedulerSupport(SchedulerSupport.NONE)
fun <R, T> Flowable<List<T>>.mapList(mapper: Function<in T, out R>): Flowable<List<R>> where T : Any, R : Any {
    return this.map { list -> list.map { mapper.apply(it) } }
}

/**
 * Similar to mapNotNull function of RxJava2.
 * Map the elements of the upstream Flowable using the [mapper] function and
 * returns only those elements not null.
 * If all the elements returned by the mapper function are null, the upstream observable
 * will fire onComplete.
 *
 * @since 3.0.5
 *
 */
@Suppress("UPPER_BOUND_VIOLATED_BASED_ON_JAVA_ANNOTATIONS")
@CheckReturnValue
@SchedulerSupport(SchedulerSupport.NONE)
fun <T, R> Flowable<T>.mapNotNull(mapper: java.util.function.Function<in T, R?>): Flowable<R> where T : Any, R : Any {
    Objects.requireNonNull(mapper, "mapper is null")
    return RxJavaPlugins.onAssembly(FlowableMapNotNull(this, mapper))
}

/**
 * Converts the source [Flowable] into a [Single]
 * @since 3.0.5
 */
fun <T> Flowable<T>.toSingle(): Single<T> where T : Any {
    return this.firstOrError()
}

/**
 * If the source [Flowable] returns a [List] of items, this transformer will
 * convert the Flowable into a [Maybe] which emit the very first item of the list,
 * if the list contains at least one element.
 * @since 3.0.5
 */
fun <T : Any> Flowable<List<T>>.firstInList(): Maybe<T> {
    return this.toSingle().firstInList()
}

/**
 * If the source [Flowable] returns a [List] of items, this transformer will
 * convert the Flowable into a [Maybe] which emit the very first item that match the predicate.
 *
 * @since 3.0.5
 */
fun <T : Any> Flowable<List<T>>.firstInList(predicate: Predicate<T>): Maybe<T> {
    return this.toSingle().firstInList(predicate)
}

/**
 * Retry the source observable with a delay.
 * @param maxAttempts maximum number of attempts
 * @param predicate predicate which given the current attempt number and the source exception should return the next delay to start a new attempt.
 *                  The return value is in milliseconds
 * @throws [RetryException] when the total number of attempts have been reached
 * @since 3.0.6
 */
fun <T> Flowable<T>.retryWhen(
    maxAttempts: Int,
    predicate: BiFunction<Throwable, Int, Long>
): Flowable<T> where T : Any {
    return this.retryWhen { flowable ->
        flowable.zipWith(Flowable.range(1, maxAttempts + 1)) { throwable, retryCount ->
            if (retryCount > maxAttempts) {
                throw RetryException(throwable)
            } else {
                predicate.apply(throwable, retryCount)
            }
        }.flatMap { delay ->
            Flowable.timer(delay, TimeUnit.MILLISECONDS)
        }
    }
}

fun <T : Any> Flowable<T>.doOnFirst(action: (T) -> Unit): Flowable<T> =
    compose(FlowableTransformers.doOnFirst(action))

fun <T : Any> Flowable<T>.doAfterFirst(action: (T) -> Unit): Flowable<T> =
    compose(FlowableTransformers.doAfterFirst(action))

fun <T : Any> Flowable<T>.doOnNth(nth: Long, action: (T) -> Unit): Flowable<T> =
    compose(FlowableTransformers.doOnNth(nth, action))

fun <T : Any> Flowable<T>.doAfterNth(nth: Long, action: (T) -> Unit): Flowable<T> =
    compose(FlowableTransformers.doAfterNth(nth, action))

/**
 * If the upstream [Flowable] fails, it re-tries subscribing to it again up to [maxRetryCount] times. The back-off time before each retry is
 * computed by calling the [backOffTimeFunc] with the current retry count. If the upstream [Flowable] fails more than [maxRetryCount] times, a
 * [MaxRetryCountExceededException] will be emitted.
 *
 * @param maxRetryCount the maximum number of retries before a [MaxRetryCountExceededException] will be emitted
 * @param backOffTimeFunc a callback that will be called to get the back-off time for the next retry (in milliseconds)
 * @return the new [Flowable] instance
 */
fun <T : Any> Flowable<T>.retryWithBackOffDelay(
    maxRetryCount: Int,
    backOffTimeFunc: (Int) -> Long
): Flowable<T> {
    return retryWhen { errors ->
        errors.zipWith(Flowable.range(1, maxRetryCount + 1)) { throwable, retryCount ->
            Pair(
                throwable,
                retryCount
            )
        }
            .flatMap { (throwable, retryCount) ->
                if (retryCount > maxRetryCount) {
                    Flowable.error(MaxRetryCountExceededException(throwable))
                } else {
                    val backOffTime = backOffTimeFunc(retryCount)
                    Flowable.timer(backOffTime, TimeUnit.MILLISECONDS)
                }
            }
    }
}
