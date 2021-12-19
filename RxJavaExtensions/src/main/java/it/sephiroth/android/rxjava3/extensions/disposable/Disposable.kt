@file:Suppress("unused")

package it.sephiroth.android.rxjava3.extensions.disposable

import io.reactivex.rxjava3.disposables.Disposable


/**
 * RxJavaExtensions
 *
 * @author Alessandro Crugnola on 28.02.21 - 18:34
 */


/**
 * Safe unsubscribe a [Disposable]
 */
fun Disposable?.disposeSafe() {
    if (this?.isDisposed == false) {
        this.dispose()
    }
}

/**
 * Returns if a [Disposable] is null or disposed
 */
fun Disposable?.isNullOrDisposed(): Boolean = this?.isDisposed ?: true

