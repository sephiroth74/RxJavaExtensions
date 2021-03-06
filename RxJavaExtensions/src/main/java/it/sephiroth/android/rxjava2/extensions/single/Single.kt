@file:Suppress("unused")

package it.sephiroth.android.rxjava2.extensions.single

import android.util.Log
import io.reactivex.Maybe
import io.reactivex.Observable
import io.reactivex.Single
import io.reactivex.android.schedulers.AndroidSchedulers
import io.reactivex.annotations.CheckReturnValue
import io.reactivex.annotations.SchedulerSupport
import it.sephiroth.android.rxjava2.extensions.observers.AutoDisposableSingleObserver


/**
 * RxJavaExtensions
 *
 * @author Alessandro Crugnola on 06.01.21 - 13:40
 */

/**
 * If the original [Single] returns a [List] of items, this transformer will
 * convert the Observable into a [Maybe] which emit the very first item of the list,
 * if the list contains at least one element.
 */
fun <T> Single<List<T>>.firstInList(): Maybe<T> {
    return this.filter { it.isNotEmpty() }.map { it.first() }
}

/**
 * Subscribe to this [Single] using an instance of the [AutoDisposableSingleObserver]
 */
fun <T> Single<T>.autoSubscribe(observer: AutoDisposableSingleObserver<T>): AutoDisposableSingleObserver<T> {
    return this.subscribeWith(observer)
}

/**
 * @see [autoSubscribe]
 */
fun <T> Single<T>.autoSubscribe(builder: (AutoDisposableSingleObserver<T>.() -> Unit)): AutoDisposableSingleObserver<T> {
    return this.subscribeWith(AutoDisposableSingleObserver(builder))
}

/**
 * alias for <code>Single.observeOn(AndroidSchedulers.mainThread())</code>
 */
fun <T> Single<T>.observeMain(): Single<T> {
    return observeOn(AndroidSchedulers.mainThread())
}


/**
 * Converts the elements of a list of a Single
 */
@CheckReturnValue
@SchedulerSupport(SchedulerSupport.NONE)
fun <R, T> Single<List<T>>.mapList(mapper: io.reactivex.functions.Function<in T, out R>) : Single<List<R>> {
    return this.map { list -> list.map { mapper.apply(it) } }
}


/**
 * Enable debug logs from a [Single], emitting
 * onNext, onError, onSubscribe and onComplete
 */
fun <T> Single<T>.debug(tag: String): Single<T> {
    return this
            .doOnError { Log.e(tag, "onError(${it.message})") }
            .doOnSubscribe { Log.v(tag, "onSubscribe()") }
            .doOnSuccess { Log.v(tag, "onSuccess()") }
            .doOnDispose { Log.w(tag, "onDispose()") }
}

fun <T> Single<T>.debugWithThread(tag: String): Single<T> {
    return this
            .doOnError { Log.e(tag, "[${Thread.currentThread().name}] onError(${it.message})") }
            .doOnSubscribe { Log.v(tag, "[${Thread.currentThread().name}] onSubscribe()") }
            .doOnSuccess { Log.v(tag, "[${Thread.currentThread().name}] onSuccess()") }
            .doOnDispose { Log.w(tag, "[${Thread.currentThread().name}] onDispose()") }
}
