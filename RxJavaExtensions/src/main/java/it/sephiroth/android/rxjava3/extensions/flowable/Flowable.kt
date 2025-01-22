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
 * Provides extension functions for the RxJava Flowable class.
 * These functions add additional functionality for working with Flowables.
 *
 * Author: Alessandro Crugnola on 06.01.21 - 13:31
 */

/**
 * Subscribe the source using an instance of the [AutoDisposableObserver].
 * The source will be disposed when a complete or error event is received.
 *
 * @param observer The AutoDisposableObserver to use for subscription.
 * @return The AutoDisposableObserver used for subscription.
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
 * Subscribe the source using an instance of the [AutoDisposableObserver].
 * The source will be disposed when a complete or error event is received.
 *
 * @param builder A lambda function to configure the AutoDisposableObserver.
 * @return The AutoDisposableObserver used for subscription.
 * @see [autoSubscribe]
 */
fun <T> Flowable<T>.autoSubscribe(
    builder: (AutoDisposableSubscriber<T>.() -> Unit)
): AutoDisposableSubscriber<T> where T : Any {
    return this.autoSubscribe(AutoDisposableSubscriber(builder))
}

/**
 * Logs the emissions of the Flowable for debugging purposes.
 *
 * @param tag The tag to use for logging.
 * @return A Flowable that logs its emissions.
 */
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

/**
 * Logs the emissions of the Flowable for debugging purposes, including the thread name.
 *
 * @param tag The tag to use for logging.
 * @return A Flowable that logs its emissions and the thread name.
 */
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
 * Alias for Flowable.observeOn(AndroidSchedulers.mainThread()).
 *
 * @return A Flowable that observes on the main thread.
 */
fun <T> Flowable<T>.observeMain(): Flowable<T> where T : Any {
    return observeOn(AndroidSchedulers.mainThread())
}

/**
 * Returns a Flowable that skips all items emitted by the source emitter
 * until a specified interval passed between each emission.
 *
 * @param time Minimum amount of time needed to pass between each interaction.
 * @param unit Time unit for the [time] parameter.
 * @param defaultOpened If true, the very first emission of the source [Flowable] will be allowed, false otherwise.
 * @return A Flowable that skips items based on the specified interval.
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
 * Returns a Flowable that filters out objects not of the type of [cls1] and [cls2].
 * Moreover, objects must alternate between cls1 and cls2, otherwise the object is skipped.
 *
 * @param cls1 The first class type to filter.
 * @param cls2 The second class type to filter.
 * @return A Flowable that emits only alternating objects of type [cls1] and [cls2].
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
 * Maps the elements of a list emitted by the source [Flowable].
 *
 * @param mapper A function to apply to each element in the list.
 * @return A Flowable that emits the mapped list.
 * @since 3.0.5
 */
@CheckReturnValue
@SchedulerSupport(SchedulerSupport.NONE)
fun <R, T> Flowable<List<T>>.mapList(mapper: Function<in T, out R>): Flowable<List<R>> where T : Any, R : Any {
    return this.map { list -> list.map { mapper.apply(it) } }
}

/**
 * Similar to mapNotNull function of RxJava2.
 * Maps the elements of the upstream Flowable using the [mapper] function and
 * returns only those elements not null.
 * If all the elements returned by the mapper function are null, the upstream observable
 * will fire onComplete.
 *
 * @param mapper A function to apply to each element in the Flowable.
 * @return A Flowable that emits only non-null elements.
 * @since 3.0.5
 */
@Suppress("UPPER_BOUND_VIOLATED_BASED_ON_JAVA_ANNOTATIONS")
@CheckReturnValue
@SchedulerSupport(SchedulerSupport.NONE)
fun <T, R> Flowable<T>.mapNotNull(mapper: java.util.function.Function<in T, R?>): Flowable<R> where T : Any, R : Any {
    Objects.requireNonNull(mapper, "mapper is null")
    return RxJavaPlugins.onAssembly(FlowableMapNotNull(this, mapper))
}

/**
 * Converts the source [Flowable] into a [Single].
 *
 * @return A Single that emits the first item of the Flowable or an error if the Flowable is empty.
 * @since 3.0.5
 */
fun <T> Flowable<T>.toSingle(): Single<T> where T : Any {
    return this.firstOrError()
}

/**
 * If the source [Flowable] returns a [List] of items, this transformer will
 * convert the Flowable into a [Maybe] which emits the very first item of the list,
 * if the list contains at least one element.
 *
 * @return A Maybe that emits the first item of the list or completes if the list is empty.
 * @since 3.0.5
 */
fun <T : Any> Flowable<List<T>>.firstInList(): Maybe<T> {
    return this.toSingle().firstInList()
}

/**
 * If the source [Flowable] returns a [List] of items, this transformer will
 * convert the Flowable into a [Maybe] which emits the very first item that matches the predicate.
 *
 * @param predicate A function that evaluates each item in the list.
 * @return A Maybe that emits the first item that matches the predicate or completes if no items match.
 * @since 3.0.5
 */
fun <T : Any> Flowable<List<T>>.firstInList(predicate: Predicate<T>): Maybe<T> {
    return this.toSingle().firstInList(predicate)
}

/**
 * Retry the source observable with a delay.
 *
 * @param maxAttempts The maximum number of attempts.
 * @param predicate A function that returns the delay before the next attempt based on the current attempt number and the source exception.
 * @return A Flowable that retries the source observable with a delay.
 * @throws [RetryException] when the total number of attempts have been reached.
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

/**
 * Applies a function to the first item emitted by the Flowable.
 *
 * @param action A function to apply to the first item.
 * @return A Flowable that applies the function to the first item.
 */
fun <T : Any> Flowable<T>.doOnFirst(action: (T) -> Unit): Flowable<T> =
    compose(FlowableTransformers.doOnFirst(action))

/**
 * Applies a function after the first item emitted by the Flowable.
 *
 * @param action A function to apply after the first item.
 * @return A Flowable that applies the function after the first item.
 */
fun <T : Any> Flowable<T>.doAfterFirst(action: (T) -> Unit): Flowable<T> =
    compose(FlowableTransformers.doAfterFirst(action))

/**
 * Applies a function to the nth item emitted by the Flowable.
 *
 * @param nth The position of the item to apply the function to.
 * @param action A function to apply to the nth item.
 * @return A Flowable that applies the function to the nth item.
 */
fun <T : Any> Flowable<T>.doOnNth(nth: Long, action: (T) -> Unit): Flowable<T> =
    compose(FlowableTransformers.doOnNth(nth, action))

/**
 * Applies a function after the nth item emitted by the Flowable.
 *
 * @param nth The position of the item after which to apply the function.
 * @param action A function to apply after the nth item.
 * @return A Flowable that applies the function after the nth item.
 */
fun <T : Any> Flowable<T>.doAfterNth(nth: Long, action: (T) -> Unit): Flowable<T> =
    compose(FlowableTransformers.doAfterNth(nth, action))

/**
 * If the upstream [Flowable] fails, it re-tries subscribing to it again up to [maxRetryCount] times. The back-off time before each retry is
 * computed by calling the [backOffTimeFunc] with the current retry count. If the upstream [Flowable] fails more than [maxRetryCount] times, a
 * [MaxRetryCountExceededException] will be emitted.
 *
 * @param maxRetryCount The maximum number of retries before a [MaxRetryCountExceededException] will be emitted.
 * @param backOffTimeFunc A callback that will be called to get the back-off time for the next retry (in milliseconds).
 * @return The new [Flowable] instance.
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
