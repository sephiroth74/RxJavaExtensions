package it.sephiroth.android.rxjava3.extensions.context

import android.content.ComponentName
import android.content.Context
import android.content.Intent
import android.content.ServiceConnection
import android.os.IBinder
import io.reactivex.rxjava3.core.Observable
import io.reactivex.rxjava3.core.ObservableEmitter
import io.reactivex.rxjava3.core.ObservableOnSubscribe
import java.util.Optional

/**
 * Factory object for creating RxJava Observables that manage Android service bindings.
 */
object RxServiceBindingFactory {

    /**
     * Binds to an Android service and returns an Observable that emits the service's IBinder.
     *
     * @param context The context to use for binding the service.
     * @param launch The intent to launch the service.
     * @param flags Binding options for the service.
     * @return An Observable that emits an Optional containing the service's IBinder.
     */
    fun bind(context: Context, launch: Intent, flags: Int): Observable<Optional<IBinder>> {
        return Observable.using({ Connection() },
            { con: Connection ->
                context.bindService(launch, con, flags)
                Observable.create(con)
            },
            { conn: ServiceConnection ->
                context.unbindService(conn)
            })
    }

    /**
     * Private class that implements ServiceConnection and ObservableOnSubscribe to manage
     * the service connection and emit the service's IBinder.
     */
    private class Connection : ServiceConnection, ObservableOnSubscribe<Optional<IBinder>> {
        private var subscriber: ObservableEmitter<Optional<IBinder>>? = null

        /**
         * Called when the service is connected.
         *
         * @param name The name of the connected service.
         * @param service The IBinder of the connected service.
         */
        override fun onServiceConnected(name: ComponentName, service: IBinder?) {
            subscriber?.onNext(Optional.ofNullable(service))
        }

        /**
         * Called when the service is disconnected.
         *
         * @param name The name of the disconnected service.
         */
        override fun onServiceDisconnected(name: ComponentName) {
            subscriber?.onComplete()
        }

        /**
         * Subscribes to the Observable and stores the emitter.
         *
         * @param observableEmitter The emitter to use for emitting items.
         * @throws Exception If an error occurs during subscription.
         */
        @Throws(Exception::class)
        override fun subscribe(observableEmitter: ObservableEmitter<Optional<IBinder>>) {
            this.subscriber = observableEmitter
        }
    }
}
