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
 * Provides extension functions for the RxJava Completable class.
 * These functions add additional functionality for working with Completables.
 *
 * Author: Alessandro Crugnola on 06.01.21 - 13:29
 */

/**
 * Subscribe to this [Completable] using an instance of the [AutoDisposableCompletableObserver].
 * Source will be automatically disposed on complete or on error.
 *
 * @param observer The AutoDisposableCompletableObserver to use for subscription.
 * @return The AutoDisposableCompletableObserver used for subscription.
 */
fun Completable.autoSubscribe(observer: AutoDisposableCompletableObserver): AutoDisposableCompletableObserver {
    return this.subscribeWith(observer)
}

/**
 * Subscribe to this [Completable] using an instance of the [AutoDisposableCompletableObserver].
 * Source will be automatically disposed on complete or on error.
 *
 * @param builder A lambda function to configure the AutoDisposableCompletableObserver.
 * @return The AutoDisposableCompletableObserver used for subscription.
 * @see [autoSubscribe]
 */
fun Completable.autoSubscribe(
    builder: (AutoDisposableCompletableObserver.() -> Unit)
): AutoDisposableCompletableObserver {
    return this.subscribeWith(AutoDisposableCompletableObserver(builder))
}

/**
 * Subscribe to this [Completable] using an instance of the [AutoDisposableCompletableObserver].
 * Source will be automatically disposed on complete or on error.
 *
 * @return The AutoDisposableCompletableObserver used for subscription.
 * @see [autoSubscribe]
 */
fun Completable.autoSubscribe(): AutoDisposableCompletableObserver {
    return this.autoSubscribe {}
}

/**
 * Alias for Completable.observeOn(AndroidSchedulers.mainThread()).
 *
 * @return A Completable that observes on the main thread.
 */
fun Completable.observeMain(): Completable {
    return observeOn(AndroidSchedulers.mainThread())
}

/**
 * Trigger a delayed action (invoked on the main thread by default).
 *
 * @param delay The delay before the action is triggered.
 * @param unit The time unit for the delay.
 * @param action The action to be triggered.
 * @return A Disposable that can be used to dispose the action.
 */
fun delay(delay: Long, unit: TimeUnit, action: () -> Unit): Disposable {
    return delay(delay, unit, AndroidSchedulers.mainThread(), action)
}

/**
 * Trigger a delayed action on the given [Scheduler].
 *
 * @param delay The delay before the action is triggered.
 * @param unit The time unit for the delay.
 * @param scheduler The scheduler to use for the delay.
 * @param action The action to be triggered.
 * @return A Disposable that can be used to dispose the action.
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

/**
 * Trigger a delayed action using a [Duration].
 *
 * @param duration The duration before the action is triggered.
 * @param action The action to be triggered.
 * @return A Disposable that can be used to dispose the action.
 */
@RequiresApi(Build.VERSION_CODES.O)
fun delay(duration: Duration, action: (() -> Unit)): Disposable =
    delay(duration.toMillis(), TimeUnit.MILLISECONDS, action)

/**
 * Trigger a delayed action on the given [Scheduler] using a [Duration].
 *
 * @param duration The duration before the action is triggered.
 * @param scheduler The scheduler to use for the delay.
 * @param action The action to be triggered.
 * @return A Disposable that can be used to dispose the action.
 */
@RequiresApi(Build.VERSION_CODES.O)
fun delay(duration: Duration, scheduler: Scheduler, action: (() -> Unit)): Disposable =
    delay(duration.toMillis(), TimeUnit.MILLISECONDS, scheduler, action)

/**
 * Trigger a delayed action on the given [Scheduler] using a [Duration].
 *
 * @param scheduler The scheduler to use for the delay.
 * @param duration The duration before the action is triggered.
 * @param action The action to be triggered.
 * @return A Disposable that can be used to dispose the action.
 */
@RequiresApi(Build.VERSION_CODES.O)
fun delay(scheduler: Scheduler, duration: Duration, action: (() -> Unit)): Disposable =
    delay(duration.toMillis(), TimeUnit.MILLISECONDS, scheduler, action)

/**
 * If the upstream [Completable] fails, it re-tries subscribing to it again up to [maxRetryCount] times. The back-off time before each retry is
 * computed by calling the [backOffTimeFunc] with the current retry count. If the upstream [Completable] fails more than [maxRetryCount] times, a
 * [MaxRetryCountExceededException] will be emitted.
 *
 * @param maxRetryCount The maximum number of retries before a [MaxRetryCountExceededException] will be emitted.
 * @param backOffTimeFunc A callback that will be called to get the back-off time for the next retry (in milliseconds).
 * @return The new [Completable] instance.
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
 *
 * @param maxAttempts The maximum number of attempts.
 * @param predicate A function that returns the delay before the next attempt based on the current attempt number and the source exception.
 * @return A Completable that retries the source observable with a delay.
 * @throws [RetryException] when the total number of attempts have been reached.
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

/**
 * Logs the emissions of the Completable for debugging purposes.
 *
 * @param tag The tag to use for logging.
 * @return A Completable that logs its emissions.
 */
@SuppressLint("LogNotTimber")
fun Completable.debug(tag: String): Completable {
    return this
        .doOnError { Log.e(tag, "onError(${it.message})") }
        .doOnSubscribe { Log.v(tag, "onSubscribe()") }
        .doOnComplete { Log.v(tag, "onComplete()") }
        .doOnDispose { Log.w(tag, "onDispose()") }
}

/**
 * Logs the emissions of the Completable for debugging purposes, including the thread name.
 *
 * @param tag The tag to use for logging.
 * @return A Completable that logs its emissions and the thread name.
 */
@SuppressLint("LogNotTimber")
fun Completable.debugWithThread(tag: String): Completable {
    return this
        .doOnError { Log.e(tag, "[${Thread.currentThread().name}] onError(${it.message})") }
        .doOnSubscribe { Log.v(tag, "[${Thread.currentThread().name}] onSubscribe()") }
        .doOnComplete { Log.v(tag, "[${Thread.currentThread().name}] onComplete()") }
        .doOnDispose { Log.w(tag, "[${Thread.currentThread().name}] onDispose()") }
}
