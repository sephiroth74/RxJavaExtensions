@file:Suppress("unused")

package it.sephiroth.android.rxjava2.extensions.completable

import android.util.Log
import io.reactivex.Completable
import io.reactivex.android.schedulers.AndroidSchedulers
import it.sephiroth.android.rxjava2.extensions.observers.AutoDisposableCompletableObserver
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
fun Completable.autoSubscribe(builder: (AutoDisposableCompletableObserver.() -> Unit)): AutoDisposableCompletableObserver {
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
fun delay(delay: Long, unit: TimeUnit, action: (() -> Unit)?): AutoDisposableCompletableObserver {
    return Completable.complete().delay(delay, unit).observeMain().autoSubscribe {
        doOnComplete { action?.invoke() }
    }
}

fun Completable.debug(tag: String): Completable {
    return this
        .doOnError { Log.e(tag, "onError(${it.message})") }
        .doOnSubscribe { Log.v(tag, "onSubscribe()") }
        .doOnComplete { Log.v(tag, "onComplete()") }
        .doOnDispose { Log.w(tag, "onDispose()") }
}

fun Completable.debugWithThread(tag: String): Completable {
    return this
        .doOnError { Log.e(tag, "[${Thread.currentThread().name}] onError(${it.message})") }
        .doOnSubscribe { Log.v(tag, "[${Thread.currentThread().name}] onSubscribe()") }
        .doOnComplete { Log.v(tag, "[${Thread.currentThread().name}] onComplete()") }
        .doOnDispose { Log.w(tag, "[${Thread.currentThread().name}] onDispose()") }
}
