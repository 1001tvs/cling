/*
 * Copyright (C) 2013 4th Line GmbH, Switzerland
 *
 * The contents of this file are subject to the terms of either the GNU
 * Lesser General Public License Version 2 or later ("LGPL") or the
 * Common Development and Distribution License Version 1 or later
 * ("CDDL") (collectively, the "License"). You may not use this file
 * except in compliance with the License. See LICENSE.txt for more
 * information.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 */

package org.fourthline.cling.android;

import android.content.BroadcastReceiver;
import android.content.Context;
import android.content.Intent;
import android.content.IntentFilter;
import android.net.ConnectivityManager;
import android.net.NetworkInfo;
import android.net.wifi.WifiManager;
import android.os.Handler;
import android.os.HandlerThread;
import android.os.Looper;
import org.fourthline.cling.UpnpServiceConfiguration;
import org.fourthline.cling.model.ModelUtil;
import org.fourthline.cling.protocol.ProtocolFactory;
import org.fourthline.cling.transport.Router;
import org.fourthline.cling.transport.RouterException;
import org.fourthline.cling.transport.RouterImpl;
import org.fourthline.cling.transport.spi.InitializationException;
import org.seamless.util.Exceptions;

import java.util.concurrent.locks.Lock;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Monitors all network connectivity changes, switching the router accordingly.
 * <p>
 * <b>Threading:</b> {@link #enable()} and {@link #disable()} bind and unbind sockets and start and
 * stop the HTTP server, so they take up to several seconds. This class never runs them on the
 * Android main thread:
 * </p>
 * <ul>
 *   <li>the connectivity broadcast receiver is registered with a dedicated
 *       "cling-android-router" thread, so
 *       {@link ConnectivityBroadcastReceiver#onReceive} is dispatched there instead of on the UI
 *       thread;</li>
 *   <li>the retries after a WIFI =&gt; NONE transition are scheduled with a delay instead of
 *       sleeping, so that thread is never blocked for seconds;</li>
 *   <li>as a last resort {@link #lock(java.util.concurrent.locks.Lock)} refuses to wait longer
 *       than {@link #MAIN_THREAD_LOCK_TIMEOUT_MILLIS} when it is called on the main thread - a UI
 *       thread waiting for the router lock is an ANR by definition.</li>
 * </ul>
 *
 * @author Michael Pujos
 * @author Christian Bauer
 */
public class AndroidRouter extends RouterImpl {

    final private static Logger log = Logger.getLogger(Router.class.getName());

    /** Thread on which connectivity changes are handled (never the Android main thread). */
    final public static String ROUTER_THREAD_NAME = "cling-android-router";

    /** How often we retry to find a new network during a WIFI =&gt; NONE =&gt; MOBILE transition. */
    final public static int NETWORK_RETRY_COUNT = 3;

    /** Delay between those retries. */
    final public static int NETWORK_RETRY_DELAY_MILLIS = 1000;

    /**
     * Upper bound for waiting on the router lock when the caller is the Android main thread.
     * Five seconds is the input dispatch ANR deadline, so we must return long before that and
     * let the caller log the "router busy" condition instead of freezing the UI.
     */
    final public static int MAIN_THREAD_LOCK_TIMEOUT_MILLIS = 100;

    final private Context context;

    final private WifiManager wifiManager;
    protected WifiManager.MulticastLock multicastLock;
    protected WifiManager.WifiLock wifiLock;
    protected NetworkInfo networkInfo;
    protected BroadcastReceiver broadcastReceiver;

    protected HandlerThread routerThread;
    protected Handler routerHandler;

    public AndroidRouter(UpnpServiceConfiguration configuration,
                         ProtocolFactory protocolFactory,
                         Context context) throws InitializationException {
        super(configuration, protocolFactory);

        this.context = context;
        this.wifiManager = ((WifiManager) context.getSystemService(Context.WIFI_SERVICE));
        this.networkInfo = NetworkUtils.getConnectedNetworkInfo(context);

        // Only register for network connectivity changes if we are not running on emulator
        if (!ModelUtil.ANDROID_EMULATOR) {
            // enable()/disable() are expensive (sockets, Jetty server) and must never run on the
            // main thread, so the receiver is dispatched on our own thread.
            this.routerThread = new HandlerThread(ROUTER_THREAD_NAME);
            this.routerThread.start();
            this.routerHandler = new Handler(this.routerThread.getLooper());

            this.broadcastReceiver = createConnectivityBroadcastReceiver();
            context.registerReceiver(
                broadcastReceiver,
                new IntentFilter("android.net.conn.CONNECTIVITY_CHANGE"),
                null,
                routerHandler
            );
        }
    }

    protected BroadcastReceiver createConnectivityBroadcastReceiver() {
		return new ConnectivityBroadcastReceiver();
	}

    @Override
    protected int getLockTimeoutMillis() {
        // Only used when the caller is not the main thread, see lock(Lock).
        return 15000;
    }

    /**
     * Bounds the time the Android main thread can spend waiting for the router lock.
     * <p>
     * Every other thread keeps the regular timeout: waiting on the router is only dangerous when
     * the waiting thread is the one that has to dispatch input events.
     * </p>
     */
    @Override
    protected void lock(Lock lock) throws RouterException {
        if (isMainThread()) {
            lock(lock, MAIN_THREAD_LOCK_TIMEOUT_MILLIS);
        } else {
            super.lock(lock);
        }
    }

    protected boolean isMainThread() {
        Looper mainLooper = Looper.getMainLooper();
        return mainLooper != null && Looper.myLooper() == mainLooper;
    }

    @Override
    public void shutdown() throws RouterException {
        // Stop reacting to connectivity changes first: the router thread must not re-enable the
        // transports while they are being torn down.
        unregisterBroadcastReceiver();
        super.shutdown();
    }

    @Override
    public boolean enable() throws RouterException {
        lock(writeLock);
        try {
            boolean enabled;
            if ((enabled = super.enable())) {
                // Enable multicast on the WiFi network interface,
                // requires android.permission.CHANGE_WIFI_MULTICAST_STATE
                if (isWifi()) {
                    setWiFiMulticastLock(true);
                    setWifiLock(true);
                }
            }
            return enabled;
        } finally {
            unlock(writeLock);
        }
    }

    @Override
    public boolean disable() throws RouterException {
        lock(writeLock);
        try {
            // Disable multicast on WiFi network interface,
            // requires android.permission.CHANGE_WIFI_MULTICAST_STATE
            if (isWifi()) {
                setWiFiMulticastLock(false);
                setWifiLock(false);
            }
            return super.disable();
        } finally {
            unlock(writeLock);
        }
    }

    public NetworkInfo getNetworkInfo() {
        return networkInfo;
    }

    public boolean isMobile() {
        return NetworkUtils.isMobile(networkInfo);
    }

    public boolean isWifi() {
        return NetworkUtils.isWifi(networkInfo);
    }

    public boolean isEthernet() {
        return NetworkUtils.isEthernet(networkInfo);
    }

    public boolean enableWiFi() {
        log.info("Enabling WiFi...");
        try {
            return wifiManager.setWifiEnabled(true);
        } catch (Throwable t) {
            // workaround (HTC One X, 4.0.3)
            //java.lang.SecurityException: Permission Denial: writing com.android.providers.settings.SettingsProvider
            // uri content://settings/system from pid=4691, uid=10226 requires android.permission.WRITE_SETTINGS
            //	at android.os.Parcel.readException(Parcel.java:1332)
            //	at android.os.Parcel.readException(Parcel.java:1286)
            //	at android.net.wifi.IWifiManager$Stub$Proxy.setWifiEnabled(IWifiManager.java:1115)
            //	at android.net.wifi.WifiManager.setWifiEnabled(WifiManager.java:946)
            log.log(Level.WARNING, "SetWifiEnabled failed", t);
            return false;
        }
    }

    /**
     * Stops listening for connectivity changes and terminates the router thread.
     * <p>
     * Safe to call more than once: the receiver is unregistered only once and pending network
     * change retries are dropped.
     * </p>
     */
    public void unregisterBroadcastReceiver() {
        if (broadcastReceiver != null) {
            try {
                context.unregisterReceiver(broadcastReceiver);
            } catch (IllegalArgumentException ex) {
                // Not (or no longer) registered - Android throws if the receiver is unknown
                log.log(Level.FINE, "Broadcast receiver was not registered: " + ex, ex);
            }
            broadcastReceiver = null;
        }
        shutdownRouterThread();
    }

    /**
     * Quits the thread which handles connectivity changes, dropping pending retries.
     */
    protected void shutdownRouterThread() {
        if (routerThread == null)
            return;

        if (routerHandler != null) {
            routerHandler.removeCallbacksAndMessages(null);
        }
        routerThread.quit();

        routerHandler = null;
        routerThread = null;
    }

    protected void setWiFiMulticastLock(boolean enable) {
        if (multicastLock == null) {
            multicastLock = wifiManager.createMulticastLock(getClass().getSimpleName());
        }

        if (enable) {
            if (multicastLock.isHeld()) {
                log.warning("WiFi multicast lock already acquired");
            } else {
                log.info("WiFi multicast lock acquired");
                multicastLock.acquire();
            }
        } else {
            if (multicastLock.isHeld()) {
                log.info("WiFi multicast lock released");
                multicastLock.release();
            } else {
                log.warning("WiFi multicast lock already released");
            }
        }
    }

    protected void setWifiLock(boolean enable) {
        if (wifiLock == null) {
            wifiLock = wifiManager.createWifiLock(WifiManager.WIFI_MODE_FULL_HIGH_PERF, getClass().getSimpleName());
        }

        if (enable) {
            if (wifiLock.isHeld()) {
                log.warning("WiFi lock already acquired");
            } else {
                log.info("WiFi lock acquired");
                wifiLock.acquire();
            }
        } else {
            if (wifiLock.isHeld()) {
                log.info("WiFi lock released");
                wifiLock.release();
            } else {
                log.warning("WiFi lock already released");
            }
        }
    }

    /**
     * Can be overriden by subclasses to do additional work.
     *
     * @param oldNetwork <code>null</code> when first called by constructor.
     */
    protected void onNetworkTypeChange(NetworkInfo oldNetwork, NetworkInfo newNetwork) throws RouterException {
        log.info(String.format("Network type changed %s => %s",
            oldNetwork == null ? "" : oldNetwork.getTypeName(),
            newNetwork == null ? "NONE" : newNetwork.getTypeName()));

        if (disable()) {
            log.info(String.format(
                "Disabled router on network type change (old network: %s)",
                oldNetwork == null ? "NONE" : oldNetwork.getTypeName()
            ));
        }

        networkInfo = newNetwork;
        if (enable()) {
            // Can return false (via earlier InitializationException thrown by NetworkAddressFactory) if
            // no bindable network address found!
            log.info(String.format(
                "Enabled router on network type change (new network: %s)",
                newNetwork == null ? "NONE" : newNetwork.getTypeName()
            ));
        }
    }

    /**
     * Handles errors when network has been switched, during reception of
     * network switch broadcast. Logs a warning by default, override to
     * change this behavior.
     */
    protected void handleRouterExceptionOnNetworkTypeChange(RouterException ex) {
        Throwable cause = Exceptions.unwrap(ex);
        if (cause instanceof InterruptedException) {
            log.log(Level.INFO, "Router was interrupted: " + ex, cause);
        } else {
            log.log(Level.WARNING, "Router error on network change: " + ex, ex);
        }
    }

    class ConnectivityBroadcastReceiver extends BroadcastReceiver {

        @Override
        public void onReceive(Context context, Intent intent) {

            if (intent == null || !ConnectivityManager.CONNECTIVITY_ACTION.equals(intent.getAction()))
                return;

            displayIntentInfo(intent);

            // This receiver is dispatched on the router thread (see the constructor), so we are
            // already off the main thread. Defensive: if a subclass registered this receiver
            // differently, never run the blocking enable()/disable() on the main thread.
            final Handler handler = routerHandler;
            if (handler != null && isMainThread()) {
                handler.post(new Runnable() {
                    @Override
                    public void run() {
                        handleNetworkChange(NETWORK_RETRY_COUNT);
                    }
                });
            } else {
                handleNetworkChange(NETWORK_RETRY_COUNT);
            }
        }

        /**
         * Applies a connectivity change, retrying a few times while the device moves between
         * networks (WIFI =&gt; NONE =&gt; MOBILE is reported as two separate changes).
         * <p>
         * The retries are scheduled with a delay instead of {@link Thread#sleep}. Sleeping here
         * used to block the Android main thread for up to three seconds, which is an ANR on its
         * own - see the "this can block the UI thread for up to 3s" note of the previous
         * implementation.
         * </p>
         *
         * @param retriesLeft How many more times we may wait for a new network to appear.
         */
        protected void handleNetworkChange(final int retriesLeft) {

            if (routerThread == null) {
                // Router is shut down, a late connectivity event must not re-enable it
                log.fine("Router thread is gone, ignoring connectivity change");
                return;
            }

            NetworkInfo newNetworkInfo = NetworkUtils.getConnectedNetworkInfo(context);
            final Handler handler = routerHandler;

            if (networkInfo != null && newNetworkInfo == null && retriesLeft > 0 && handler != null) {
                log.warning(String.format(
                    "%s => NONE network transition, waiting for new network... retry #%d",
                    networkInfo.getTypeName(), NETWORK_RETRY_COUNT - retriesLeft + 1
                ));
                handler.postDelayed(new Runnable() {
                    @Override
                    public void run() {
                        handleNetworkChange(retriesLeft - 1);
                    }
                }, NETWORK_RETRY_DELAY_MILLIS);
                return;
            }

            if (isSameNetworkType(networkInfo, newNetworkInfo)) {
                log.info("No actual network change... ignoring event!");
                return;
            }

            try {
                onNetworkTypeChange(networkInfo, newNetworkInfo);
            } catch (RouterException ex) {
                handleRouterExceptionOnNetworkTypeChange(ex);
            }
        }

        protected boolean isSameNetworkType(NetworkInfo network1, NetworkInfo network2) {
            if (network1 == null && network2 == null)
                return true;
            if (network1 == null || network2 == null)
                return false;
            return network1.getType() == network2.getType();
        }

        protected void displayIntentInfo(Intent intent) {
            boolean noConnectivity = intent.getBooleanExtra(ConnectivityManager.EXTRA_NO_CONNECTIVITY, false);
            String reason = intent.getStringExtra(ConnectivityManager.EXTRA_REASON);
            boolean isFailover = intent.getBooleanExtra(ConnectivityManager.EXTRA_IS_FAILOVER, false);

            NetworkInfo currentNetworkInfo = (NetworkInfo) intent.getParcelableExtra(ConnectivityManager.EXTRA_NETWORK_INFO);
            NetworkInfo otherNetworkInfo = (NetworkInfo) intent.getParcelableExtra(ConnectivityManager.EXTRA_OTHER_NETWORK_INFO);

            log.info("Connectivity change detected...");
            log.info("EXTRA_NO_CONNECTIVITY: " + noConnectivity);
            log.info("EXTRA_REASON: " + reason);
            log.info("EXTRA_IS_FAILOVER: " + isFailover);
            log.info("EXTRA_NETWORK_INFO: " + (currentNetworkInfo == null ? "none" : currentNetworkInfo));
            log.info("EXTRA_OTHER_NETWORK_INFO: " + (otherNetworkInfo == null ? "none" : otherNetworkInfo));
            log.info("EXTRA_EXTRA_INFO: " + intent.getStringExtra(ConnectivityManager.EXTRA_EXTRA_INFO));
        }

    }

}
