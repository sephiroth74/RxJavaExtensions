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

package it.sephiroth.android.rxjava3.extensions.observable

import android.annotation.SuppressLint
import android.os.Build
import android.util.Log
import androidx.annotation.RequiresApi
import io.reactivex.rxjava3.android.schedulers.AndroidSchedulers
import io.reactivex.rxjava3.annotations.CheckReturnValue
import io.reactivex.rxjava3.annotations.SchedulerSupport
import io.reactivex.rxjava3.core.Maybe
import io.reactivex.rxjava3.core.Observable
import io.reactivex.rxjava3.core.Scheduler
import io.reactivex.rxjava3.core.Single
import io.reactivex.rxjava3.functions.BiFunction
import io.reactivex.rxjava3.functions.Function
import io.reactivex.rxjava3.kotlin.Observables
import io.reactivex.rxjava3.plugins.RxJavaPlugins
import io.reactivex.rxjava3.schedulers.Schedulers
import it.sephiroth.android.rxjava3.extensions.MaxRetryCountExceededException
import it.sephiroth.android.rxjava3.extensions.MuteException
import it.sephiroth.android.rxjava3.extensions.RetryException
import it.sephiroth.android.rxjava3.extensions.observers.AutoDisposableObserver
import it.sephiroth.android.rxjava3.extensions.operators.ObservableMapNotNull
import it.sephiroth.android.rxjava3.extensions.operators.ObservableTransformers
import it.sephiroth.android.rxjava3.extensions.single.firstInList
import java.time.Duration
import java.util.Objects
import java.util.concurrent.TimeUnit
import java.util.function.Predicate

/**
 * RxJavaExtensions
 *
 * @author Alessandro Crugnola on 06.01.21 - 13:35
 */

/**
 * Converts an [Observable] into a [Single]
 */
fun <T> Observable<T>.toSingle(): Single<T> where T : Any {
    return this.firstOrError()
}

/**
 * If the original [Observable] returns a [List] of items, this transformer will
 * convert the Observable into a [Maybe] which emit the very first item of the list,
 * if the list contains at least one element.
 */
fun <T : Any> Observable<List<T>>.firstInList(): Maybe<T> {
    return this.toSingle().firstInList()
}

/**
 * If the original [Observable] returns a [List] of items, this transformer will
 * convert the Observable into a [Maybe] which emit the very first item that match the predicate.
 *
 * @since 3.0.5
 */
fun <T : Any> Observable<List<T>>.firstInList(predicate: Predicate<T>): Maybe<T> {
    return this.toSingle().firstInList(predicate)
}

/**
 * Subscribe the source using an instance of the [AutoDisposableObserver].
 * The source will be disposed when a complete or error event is received.
 */
fun <T> Observable<T>.autoSubscribe(observer: AutoDisposableObserver<T>): AutoDisposableObserver<T> where T : Any {
    var observable = this
    observer._doOnFirst?.let {
        observable = observable.doOnFirst(it)
    }
    observer._doAfterFirst?.let {
        observable = observable.doAfterFirst(it)
    }
    return observable.subscribeWith(observer)
}

/**
 * @see [autoSubscribe]
 */
fun <T> Observable<T>.autoSubscribe(
    builder: (AutoDisposableObserver<T>.() -> Unit)
): AutoDisposableObserver<T> where T : Any {
    return this.autoSubscribe(AutoDisposableObserver(builder))
}

/**
 * alias for Observable.observeOn(AndroidSchedulers.mainThread())
 */
fun <T> Observable<T>.observeMain(): Observable<T> where T : Any {
    return observeOn(AndroidSchedulers.mainThread())
}

/**
 * Retries the source observable when the predicate succeeds.
 *
 * @param predicate when the predicate returns true a new attempt will be made from the source observable
 * @param maxRetry maximum number of attempts
 * @param delayBeforeRetry minimum time (see [timeUnit]) before the next attempt
 * @param timeUnit time unit for the [delayBeforeRetry] param
 */
fun <T> Observable<T>.retry(
    predicate: (Throwable) -> Boolean,
    maxRetry: Int,
    delayBeforeRetry: Long,
    timeUnit: TimeUnit
): Observable<T> where T : Any =
    retryWhen { observable ->
        Observables.zip(
            observable.map { if (predicate(it)) it else throw it },
            Observable.interval(delayBeforeRetry, timeUnit)
        ).map { if (it.second >= maxRetry) throw it.first }
    }

/**
 * Retries the source observable with a back-off delay.
 *
 * @param maxRetryCount The maximum number of retry attempts.
 * @param backOffTimeFunc A function that returns the back-off time in milliseconds for each retry attempt.
 * @return An Observable that retries the source observable with a back-off delay.
 */
fun <T : Any> Observable<T>.retryWithBackOffDelay(maxRetryCount: Int, backOffTimeFunc: (Int) -> Long): Observable<T> {
    return retryWhen { errors ->
        errors.zipWith(Observable.range(1, maxRetryCount + 1)) { throwable, retryCount -> Pair(throwable, retryCount) }
            .flatMap { (throwable, retryCount) ->
                if (retryCount > maxRetryCount) {
                    Observable.error(MaxRetryCountExceededException(throwable))
                } else {
                    val backOffTime: Long = backOffTimeFunc(retryCount)
                    Observable.timer(backOffTime, TimeUnit.MILLISECONDS)
                }
            }
    }
}

/**
 * Returns an Observable that emits the source observable every [time]. The source observable is triggered immediately
 * and all the consecutive calls after the time specified.
 *
 * @param time The interval time between emissions.
 * @param timeUnit The time unit for the interval.
 * @param scheduler The scheduler to use for the interval.
 * @return An Observable that emits the source observable at the specified interval.
 */
fun <T> Observable<T>.refreshEvery(
    time: Long,
    timeUnit: TimeUnit,
    scheduler: Scheduler = Schedulers.computation()
): Observable<T> where T : Any =
    Observable.interval(0, time, timeUnit, scheduler).flatMap { this }

/**
 * Returns an Observable that emits the source observable every time the [publisher] observable emits true.
 *
 * @param publisher The publisher observable that triggers the source observable.
 * @return An Observable that emits the source observable when the publisher emits true.
 */
fun <T> Observable<T>.autoRefresh(publisher: Observable<Boolean>): Observable<T> where T : Any {
    return publisher.filter { it }.flatMap { this }
}

/**
 * Returns an Observable that emits the source observable at a fixed interval.
 *
 * @param initialDelay The initial delay before the first emission.
 * @param period The interval period between emissions.
 * @param unit The time unit for the interval.
 * @return An Observable that emits the source observable at the specified interval.
 */
fun <T : Any> Observable<T>.autoRefresh(
    initialDelay: Long = 0,
    period: Long = 1,
    unit: TimeUnit = TimeUnit.MINUTES
): Observable<T> {
    return Observable.interval(initialDelay, period, unit).switchMap { this }
}

/**
 * Converts the elements of a list of an Observable.
 *
 * @param mapper A function to apply to each element in the list.
 * @return An Observable that emits the mapped list.
 */
@CheckReturnValue
@SchedulerSupport(SchedulerSupport.NONE)
fun <R, T> Observable<List<T>>.mapList(mapper: Function<in T, out R>): Observable<List<R>> where T : Any, R : Any {
    return this.map { list -> list.map { mapper.apply(it) } }
}

/**
 * Mute the source [Observable] until the predicate [func] returns true, retrying using the given [delay].
 *
 * @param delay The delay before retrying.
 * @param unit The time unit for the delay.
 * @param func The predicate function to evaluate.
 * @return An Observable that mutes the source observable until the predicate returns true.
 */
fun <T> Observable<T>.muteUntil(
    delay: Long,
    unit: TimeUnit,
    func: () -> Boolean
): Observable<T> where T : Any {
    return this.doOnNext { if (func()) throw MuteException() }
        .retryWhen { t: Observable<Throwable> ->
            t.flatMap { error: Throwable ->
                if (error is MuteException) Observable.timer(delay, unit)
                else Observable.error(error)
            }
        }
}

/**
 * Similar to mapNotNull of RxJava2.
 * Map the elements of the upstream Observable using the [mapper] function and
 * returns only those elements not null.
 * If all the elements returned by the mapper function are null, the upstream observable
 * will fire onComplete.
 *
 * @param mapper A function to apply to each element in the Observable.
 * @return An Observable that emits only non-null elements.
 * @since 3.0.3
 */
@CheckReturnValue
@SchedulerSupport(SchedulerSupport.NONE)
fun <T, R> Observable<T>.mapNotNull(
    mapper: java.util.function.Function<in T, R?>
): Observable<R> where T : Any, R : Any {
    Objects.requireNonNull(mapper, "mapper is null")
    val o = ObservableMapNotNull(this, mapper)
    return RxJavaPlugins.onAssembly(o)
}

/**
 * Logs the emissions of the Observable for debugging purposes.
 *
 * @param tag The tag to use for logging.
 * @return An Observable that logs its emissions.
 */
@SuppressLint("LogNotTimber")
fun <T> Observable<T>.debug(tag: String): Observable<T> where T : Any {
    return this
        .doOnNext { Log.v(tag, "onNext($it)") }
        .doOnError { Log.e(tag, "onError(${it.message})") }
        .doOnSubscribe { Log.v(tag, "onSubscribe()") }
        .doOnComplete { Log.v(tag, "onComplete()") }
        .doOnDispose { Log.w(tag, "onDispose()") }
}

/**
 * Logs the emissions of the Observable for debugging purposes, including the thread name.
 *
 * @param tag The tag to use for logging.
 * @return An Observable that logs its emissions and the thread name.
 */
@SuppressLint("LogNotTimber")
fun <T> Observable<T>.debugWithThread(tag: String): Observable<T> where T : Any {
    return this
        .doOnNext { Log.v(tag, "[${Thread.currentThread().name}] onNext($it)") }
        .doOnError { Log.e(tag, "[${Thread.currentThread().name}] onError(${it.message})") }
        .doOnSubscribe { Log.v(tag, "[${Thread.currentThread().name}] onSubscribe()") }
        .doOnComplete { Log.v(tag, "[${Thread.currentThread().name}] onComplete()") }
        .doOnDispose { Log.w(tag, "[${Thread.currentThread().name}] onDispose()") }
}

/**
 * Retry the source observable with a delay.
 *
 * @param maxAttempts The maximum number of attempts.
 * @param predicate A function that returns the delay before the next attempt based on the current attempt number and the source exception.
 * @return An Observable that retries the source observable with a delay.
 * @throws [RetryException] when the total number of attempts have been reached.
 * @since 3.0.6
 */
fun <T> Observable<T>.retryWhen(
    maxAttempts: Int,
    predicate: BiFunction<Throwable, Int, Long>
): Observable<T> where T : Any {
    return this.retryWhen { observable ->
        observable.zipWith(Observable.range(1, maxAttempts + 1)) { throwable, retryCount ->
            if (retryCount > maxAttempts) {
                throw RetryException(throwable)
            } else {
                predicate.apply(throwable, retryCount)
            }
        }.flatMap { delay -> Observable.timer(delay, TimeUnit.MILLISECONDS) }
    }
}

/**
 * Applies a function to the first item emitted by the Observable.
 *
 * @param action A function to apply to the first item.
 * @return An Observable that applies the function to the first item.
 */
fun <T : Any> Observable<T>.doOnFirst(action: (T) -> Unit): Observable<T> =
    compose(ObservableTransformers.doOnFirst(action))

/**
 * Applies a function to the nth item emitted by the Observable.
 *
 * @param nth The position of the item to apply the function to.
 * @param action A function to apply to the nth item.
 * @return An Observable that applies the function to the nth item.
 */
fun <T : Any> Observable<T>.doOnNth(nth: Long, action: (T) -> Unit): Observable<T> =
    compose(ObservableTransformers.doOnNth(nth, action))

/**
 * Applies a function after the nth item emitted by the Observable.
 *
 * @param nth The position of the item after which to apply the function.
 * @param action A function to apply after the nth item.
 * @return An Observable that applies the function after the nth item.
 */
fun <T : Any> Observable<T>.doAfterNth(nth: Long, action: (T) -> Unit): Observable<T> =
    compose(ObservableTransformers.doAfterNth(nth, action))

/**
 * Applies a function after the first item emitted by the Observable.
 *
 * @param action A function to apply after the first item.
 * @return An Observable that applies the function after the first item.
 */
fun <T : Any> Observable<T>.doAfterFirst(action: (T) -> Unit): Observable<T> =
    compose(ObservableTransformers.doAfterFirst(action))

/**
 * Debounces the source Observable from the specified index with the given timeout.
 *
 * @param index The index from which to start debouncing.
 * @param timeout The duration of the debounce timeout.
 * @return An Observable that debounces the source Observable from the specified index.
 */
@RequiresApi(Build.VERSION_CODES.O)
fun <T : Any> Observable<T>.debounceFrom(index: Long = 1, timeout: Duration): Observable<T> {
    return this.publish {
        it.take(index).concatWith(it.debounce(timeout.toMillis(), TimeUnit.MILLISECONDS))
    }
}

/**
 * Debounces the source Observable from the specified index with the given timeout.
 *
 * @param index The index from which to start debouncing.
 * @param timeout The duration of the debounce timeout.
 * @param unit The time unit for the timeout.
 * @return An Observable that debounces the source Observable from the specified index.
 */
fun <T : Any> Observable<T>.debounceFrom(
    index: Long = 1,
    timeout: Long,
    unit: TimeUnit
): Observable<T> {
    return this.publish { it.take(index).concatWith(it.debounce(timeout, unit)) }
}

/**
 * Applies a timeout to the first item emitted by the Observable.
 *
 * @param timeout The duration of the timeout.
 * @param unit The time unit for the timeout.
 * @return An Observable that applies a timeout to the first item.
 */
fun <T : Any> Observable<T>.timeoutFirstOnly(timeout: Long, unit: TimeUnit): Observable<T> {
    return this.timeout<Long, Long>(
        Observable.timer(timeout, unit)
    ) { Observable.never() }
}

/**
 * Emits pairs of the previous and current items emitted by the Observable.
 *
 * @return An Observable that emits pairs of the previous and current items.
 */
fun <T : Any> Observable<T>.withPrevious(): Observable<Pair<T?, T>> {
    return this.scan(Pair<T?, T?>(null, null)) { previous, current ->
        Pair(previous.second, current)
    }.skip(1).map {
        it.first to it.second!!
    }
}
