@file:Suppress("unused")

package it.sephiroth.android.rxjava2.extensions.flowable


import android.util.Log
import io.reactivex.Flowable
import io.reactivex.android.schedulers.AndroidSchedulers
import java.util.concurrent.TimeUnit

/**
 * RxJavaExtensions
 *
 * @author Alessandro Crugnola on 06.01.21 - 13:31
 */

fun <T> Flowable<T>.debug(tag: String): Flowable<T> {
    return this
        .doOnNext { Log.v(tag, "onNext($it)") }
        .doOnError { Log.e(tag, "onError(${it.message})") }
        .doOnSubscribe { Log.v(tag, "onSubscribe()") }
        .doOnComplete { Log.v(tag, "onComplete()") }
        .doOnCancel { Log.w(tag, "onCancel()") }
        .doOnRequest { Log.w(tag, "onRequest()") }
        .doOnTerminate { Log.w(tag, "onTerminate()") }
}

fun <T> Flowable<T>.debugWithThread(tag: String): Flowable<T> {
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
fun <T> Flowable<T>.observeMain(): Flowable<T> {
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
    defaultOpened: Boolean = true
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
