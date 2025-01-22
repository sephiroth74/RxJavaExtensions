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

package it.sephiroth.android.rxjava3.extensions.completable

import android.annotation.SuppressLint
import android.os.Build
import android.util.Log
import androidx.annotation.RequiresApi
import io.reactivex.rxjava3.android.schedulers.AndroidSchedulers
import io.reactivex.rxjava3.core.Completable
import io.reactivex.rxjava3.core.Flowable
import io.reactivex.rxjava3.core.Scheduler
import io.reactivex.rxjava3.disposables.Disposable
import io.reactivex.rxjava3.functions.BiFunction
import it.sephiroth.android.rxjava3.extensions.MaxRetryCountExceededException
import it.sephiroth.android.rxjava3.extensions.RetryException
import it.sephiroth.android.rxjava3.extensions.observers.AutoDisposableCompletableObserver
import java.time.Duration
import java.util.concurrent.TimeUnit

/**
 * RxJavaExtensions
 *
 * @author Alessandro Crugnola on 06.01.21 - 13:29
 */

/**
 * Subscribe to this [Completable] using an instance of the [AutoDisposableCompletableObserver].
 * Source will be automatically disposed on complete or on error.
 */
fun Completable.autoSubscribe(observer: AutoDisposableCompletableObserver): AutoDisposableCompletableObserver {
    return this.subscribeWith(observer)
}

/**
 * @see [autoSubscribe]
 */
fun Completable.autoSubscribe(
    builder: (AutoDisposableCompletableObserver.() -> Unit)
): AutoDisposableCompletableObserver {
    return this.subscribeWith(AutoDisposableCompletableObserver(builder))
}

/**
 * @see [autoSubscribe]
 */
fun Completable.autoSubscribe(): AutoDisposableCompletableObserver {
    return this.autoSubscribe {}
}

/**
 * alias for <code>Completable.observeOn(AndroidSchedulers.mainThread())</code>
 */
fun Completable.observeMain(): Completable {
    return observeOn(AndroidSchedulers.mainThread())
}

/**
 * Trigger a delayed action (invoked on the main thread by default)
 */
fun delay(delay: Long, unit: TimeUnit, action: () -> Unit): Disposable {
    return delay(delay, unit, AndroidSchedulers.mainThread(), action)
}

/**
 * Trigger a delayed action on the given [Scheduler]
 */
fun delay(delay: Long, unit: TimeUnit, scheduler: Scheduler, action: () -> Unit): Disposable {
    return if (delay <= 0L) {
        scheduler.scheduleDirect(action)
        Completable.complete().subscribe()
    } else {
        Completable.complete().delay(delay, unit).observeOn(scheduler).autoSubscribe {
            doOnComplete { action.invoke() }
        }
    }
}

@RequiresApi(Build.VERSION_CODES.O)
fun delay(duration: Duration, action: (() -> Unit)): Disposable =
    delay(duration.toMillis(), TimeUnit.MILLISECONDS, action)

@RequiresApi(Build.VERSION_CODES.O)
fun delay(duration: Duration, scheduler: Scheduler, action: (() -> Unit)): Disposable =
    delay(duration.toMillis(), TimeUnit.MILLISECONDS, scheduler, action)

@RequiresApi(Build.VERSION_CODES.O)
fun delay(scheduler: Scheduler, duration: Duration, action: (() -> Unit)): Disposable =
    delay(duration.toMillis(), TimeUnit.MILLISECONDS, scheduler, action)

/**
 * If the upstream [Completable] fails, it re-tries subscribing to it again up to [maxRetryCount] times. The back-off time before each retry is
 * computed by calling the [backOffTimeFunc] with the current retry count. If the upstream [Completable] fails more than [maxRetryCount] times, a
 * [MaxRetryCountExceededException] will be emitted.
 *
 * @param maxRetryCount the maximum number of retries before a [MaxRetryCountExceededException] will be emitted
 * @param backOffTimeFunc a callback that will be called to get the back-off time for the next retry (in milliseconds)
 * @return the new [Completable] instance
 */
fun Completable.retryWithBackOffDelay(
    maxRetryCount: Int,
    backOffTimeFunc: (Int) -> Long
): Completable {
    return retryWhen { errors ->
        errors.zipWith(Flowable.range(1, maxRetryCount + 1)) { throwable, retryCount -> Pair(throwable, retryCount) }
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

/**
 * Retry the source observable with a delay.
 * @param maxAttempts maximum number of attempts
 * @param predicate predicate which given the current attempt number and the source exception should return the next delay to start a new attempt.
 *                  The return value is in milliseconds
 * @throws [RetryException] when the total number of attempts have been reached
 * @since 3.0.6
 */
fun Completable.retryWhen(maxAttempts: Int, predicate: BiFunction<Throwable, Int, Long>): Completable {
    return this.retryWhen { flowable ->
        flowable.zipWith(Flowable.range(1, maxAttempts + 1)) { throwable, retryCount ->
            if (retryCount > maxAttempts) {
                throw RetryException(throwable)
            } else {
                predicate.apply(throwable, retryCount)
            }
        }.flatMap { delay -> Flowable.timer(delay, TimeUnit.MILLISECONDS) }
    }
}

@SuppressLint("LogNotTimber")
fun Completable.debug(tag: String): Completable {
    return this
        .doOnError { Log.e(tag, "onError(${it.message})") }
        .doOnSubscribe { Log.v(tag, "onSubscribe()") }
        .doOnComplete { Log.v(tag, "onComplete()") }
        .doOnDispose { Log.w(tag, "onDispose()") }
}

@SuppressLint("LogNotTimber")
fun Completable.debugWithThread(tag: String): Completable {
    return this
        .doOnError { Log.e(tag, "[${Thread.currentThread().name}] onError(${it.message})") }
        .doOnSubscribe { Log.v(tag, "[${Thread.currentThread().name}] onSubscribe()") }
        .doOnComplete { Log.v(tag, "[${Thread.currentThread().name}] onComplete()") }
        .doOnDispose { Log.w(tag, "[${Thread.currentThread().name}] onDispose()") }
}
