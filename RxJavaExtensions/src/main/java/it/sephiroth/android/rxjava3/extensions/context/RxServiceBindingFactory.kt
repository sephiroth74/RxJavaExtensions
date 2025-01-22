package it.sephiroth.android.rxjava3.extensions.context

import android.content.ComponentName
import android.content.Context
import android.content.Intent
import android.content.ServiceConnection
import android.os.IBinder
import android.util.Log
import io.reactivex.rxjava3.core.Observable
import io.reactivex.rxjava3.core.ObservableEmitter
import io.reactivex.rxjava3.core.ObservableOnSubscribe
import java.util.Optional

object RxServiceBindingFactory {
    fun bind(context: Context, launch: Intent, flags: Int): Observable<Optional<IBinder>> {
        return Observable.using({ Connection() },
            { con: Connection ->
                Log.d("RxServiceBindingFactory", "bind($launch, $flags)")
                context.bindService(launch, con, flags)
                Observable.create(con)
            },
            { conn: ServiceConnection ->
                Log.d("RxServiceBindingFactory", "unbind($conn)")
                context.unbindService(conn)
            })
    }

    private class Connection : ServiceConnection, ObservableOnSubscribe<Optional<IBinder>> {
        private var subscriber: ObservableEmitter<Optional<IBinder>>? = null

        override fun onServiceConnected(name: ComponentName, service: IBinder?) {
            Log.i("RxServiceBindingFactory", "onServiceConnected($name, $service)")
            subscriber?.onNext(Optional.ofNullable(service))
        }

        override fun onServiceDisconnected(name: ComponentName) {
            Log.i("RxServiceBindingFactory", "onServiceDisconnected($name)")
            subscriber?.onComplete()
        }

        @Throws(Exception::class)
        override fun subscribe(observableEmitter: ObservableEmitter<Optional<IBinder>>) {
            Log.i("RxServiceBindingFactory", "subscribe($observableEmitter)")
            this.subscriber = observableEmitter
        }
    }
}
