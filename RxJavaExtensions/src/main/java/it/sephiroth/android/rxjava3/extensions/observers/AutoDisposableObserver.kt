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
 * FITNESS FOR A PARTICULAR PURPOSE AND NON INFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package it.sephiroth.android.rxjava3.extensions.observers

/**
 * Auto Disposable Observer.
 * Upon completion, or error, will automatically unsubscribe from the
 * upstream
 *
 * @author Alessandro Crugnola on 06.01.21 - 13:21
 */
@Suppress("unused")
class AutoDisposableObserver<T : Any>() : DisposableObserver<T>() {

    private var _doOnNext: ((T) -> Unit)? = null
    private var _doOnStart: (() -> Unit)? = null
    private var _doOnComplete: (() -> Unit)? = null
    private var _doOnError: ((Throwable) -> Unit)? = null
    private var _doOnFinish: (() -> Unit)? = null
    private var _doOnDispose: (() -> Unit)? = null
    internal var _doOnFirst: ((T) -> Unit)? = null
    internal var _doAfterFirst: ((T) -> Unit)? = null

    @Suppress("unused")
    constructor(builder: (AutoDisposableObserver<T>.() -> Unit)) : this() {
        this.builder()
    }

    fun onFirst(t: T) {
        _doOnFirst?.invoke(t)
    }

    fun onAfterFirst(t: T) {
        _doAfterFirst?.invoke(t)
    }

    override fun onNext(t: T) {
        _doOnNext?.invoke(t)
    }

    override fun onComplete() {
        _doOnComplete?.invoke()
        _doOnFinish?.invoke()
        dispose()
    }

    override fun onStart() {
        _doOnStart?.invoke()
    }

    override fun onError(e: Throwable) {
        _doOnError?.invoke(e)
        _doOnFinish?.invoke()
        dispose()
    }

    override fun onDispose() {
        _doOnDispose?.invoke()
        clear()
    }

    fun doOnFirst(t: ((t: T) -> Unit)) {
        _doOnFirst = t
    }

    fun doAfterFirst(t: ((t: T) -> Unit)) {
        _doAfterFirst = t
    }

    fun doOnStart(t: (() -> Unit)) {
        _doOnStart = t
    }

    fun doOnNext(t: ((T) -> Unit)) {
        _doOnNext = t
    }

    fun doOnComplete(t: (() -> Unit)) {
        _doOnComplete = t
    }

    fun doOnError(t: ((Throwable) -> Unit)) {
        _doOnError = t
    }

    fun doOnFinish(t: (() -> Unit)) {
        _doOnFinish = t
    }

    fun doOnDispose(t: (() -> Unit)) {
        _doOnDispose = t
    }

    private fun clear() {
        _doOnNext = null
        _doOnStart = null
        _doOnComplete = null
        _doOnError = null
        _doOnFinish = null
        _doOnDispose = null
        _doOnFirst = null
        _doAfterFirst = null
    }
}
