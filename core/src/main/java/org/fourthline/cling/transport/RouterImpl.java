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

package org.fourthline.cling.transport;

import org.fourthline.cling.UpnpServiceConfiguration;
import org.fourthline.cling.model.NetworkAddress;
import org.fourthline.cling.model.message.IncomingDatagramMessage;
import org.fourthline.cling.model.message.OutgoingDatagramMessage;
import org.fourthline.cling.model.message.StreamRequestMessage;
import org.fourthline.cling.model.message.StreamResponseMessage;
import org.fourthline.cling.protocol.ProtocolCreationException;
import org.fourthline.cling.protocol.ProtocolFactory;
import org.fourthline.cling.protocol.ReceivingAsync;
import org.fourthline.cling.transport.spi.DatagramIO;
import org.fourthline.cling.transport.spi.InitializationException;
import org.fourthline.cling.transport.spi.MulticastReceiver;
import org.fourthline.cling.transport.spi.NetworkAddressFactory;
import org.fourthline.cling.transport.spi.NoNetworkException;
import org.fourthline.cling.transport.spi.StreamClient;
import org.fourthline.cling.transport.spi.StreamServer;
import org.fourthline.cling.transport.spi.UpnpStream;
import org.seamless.util.Exceptions;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;
import javax.enterprise.inject.Default;
import javax.inject.Inject;
import java.net.BindException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Default implementation of network message router.
 * <p>
 * Initializes and starts listening for data on the network when enabled.
 * </p>
 * <p>
 * <b>Locking rule:</b> the router lock (<code>readLock</code>/<code>writeLock</code>) guards the
 * lifecycle of the transports and the membership of the transport maps only. It must never be
 * held while blocking network I/O is performed. A single HTTP request can block for up to
 * {@link org.fourthline.cling.transport.spi.StreamClientConfiguration#getTimeoutSeconds()}
 * (60 seconds by default) when the remote device is unreachable; if that request were made while
 * holding the read lock, then {@link #enable()} and {@link #disable()} - and on Android the
 * application main thread inside
 * {@link org.fourthline.cling.android.AndroidRouter#onNetworkTypeChange} - would queue up behind
 * it. That is the "Input dispatching timed out" ANR reported on the Play Store (main thread
 * parked in <code>RouterImpl.lock()</code> because of a connectivity change broadcast).
 * </p>
 * <p>
 * Outgoing sends therefore copy what they need while holding the lock, release it, and only then
 * talk to the network. {@link #enable()}/{@link #disable()} keep the exclusive write lock for the
 * whole start/stop sequence, as that sequence is the only thing they guard; they must be invoked
 * from a background thread (on Android: never from the main thread).
 * </p>
 *
 * @author Christian Bauer
 */
@ApplicationScoped
public class RouterImpl implements Router {

    private static Logger log = Logger.getLogger(Router.class.getName());

    protected UpnpServiceConfiguration configuration;
    protected ProtocolFactory protocolFactory;

    protected volatile boolean enabled;
    protected ReentrantReadWriteLock routerLock = new ReentrantReadWriteLock(true);
    protected Lock readLock = routerLock.readLock();
    protected Lock writeLock = routerLock.writeLock();

    // These are created/destroyed when the router is enabled/disabled
    protected NetworkAddressFactory networkAddressFactory;
    protected StreamClient streamClient;
    protected final Map<NetworkInterface, MulticastReceiver> multicastReceivers = new HashMap<>();
    protected final Map<InetAddress, DatagramIO> datagramIOs = new HashMap<>();
    protected final Map<InetAddress, StreamServer> streamServers = new HashMap<>();

    protected RouterImpl() {
    }

    /**
     * @param configuration   The configuration used by this router.
     * @param protocolFactory The protocol factory used by this router.
     */
    @Inject
    public RouterImpl(UpnpServiceConfiguration configuration, ProtocolFactory protocolFactory) {
        log.info("Creating Router: " + getClass().getName());
        this.configuration = configuration;
        this.protocolFactory = protocolFactory;
    }

    public boolean enable(@Observes @Default EnableRouter event) throws RouterException {
        return enable();
    }

    public boolean disable(@Observes @Default DisableRouter event) throws RouterException {
        return disable();
    }

    public UpnpServiceConfiguration getConfiguration() {
        return configuration;
    }

    public ProtocolFactory getProtocolFactory() {
        return protocolFactory;
    }

    /**
     * Initializes listening services: First an instance of {@link org.fourthline.cling.transport.spi.MulticastReceiver}
     * is bound to each network interface. Then an instance of {@link org.fourthline.cling.transport.spi.DatagramIO} and
     * {@link org.fourthline.cling.transport.spi.StreamServer} is bound to each bind address returned by the network
     * address factory, respectively. There is only one instance of
     * {@link org.fourthline.cling.transport.spi.StreamClient} created and managed by this router.
     */
    @Override
    public boolean enable() throws RouterException {
        lock(writeLock);
        try {
            if (!enabled) {
                try {
                    log.fine("Starting networking services...");
                    networkAddressFactory = getConfiguration().createNetworkAddressFactory();

                    startInterfaceBasedTransports(networkAddressFactory.getNetworkInterfaces());
                    startAddressBasedTransports(networkAddressFactory.getBindAddresses());

                    // The transports possibly removed some unusable network interfaces/addresses
                    if (!networkAddressFactory.hasUsableNetwork()) {
                        throw new NoNetworkException(
                            "No usable network interface and/or addresses available, check the log for errors."
                        );
                    }

                    // Start the HTTP client last, we don't even have to try if there is no network
                    streamClient = getConfiguration().createStreamClient();

                    enabled = true;
                    return true;
                } catch (InitializationException ex) {
                    handleStartFailure(ex);
                }
            }
            return false;
        } finally {
            unlock(writeLock);
        }
    }

    @Override
    public boolean disable() throws RouterException {
        lock(writeLock);
        try {
            if (enabled) {
                log.fine("Disabling network services...");

                if (streamClient != null) {
                    log.fine("Stopping stream client connection management/pool");
                    streamClient.stop();
                    streamClient = null;
                }

                for (Map.Entry<InetAddress, StreamServer> entry : streamServers.entrySet()) {
                    log.fine("Stopping stream server on address: " + entry.getKey());
                    entry.getValue().stop();
                }
                streamServers.clear();

                for (Map.Entry<NetworkInterface, MulticastReceiver> entry : multicastReceivers.entrySet()) {
                    log.fine("Stopping multicast receiver on interface: " + entry.getKey().getDisplayName());
                    entry.getValue().stop();
                }
                multicastReceivers.clear();

                for (Map.Entry<InetAddress, DatagramIO> entry : datagramIOs.entrySet()) {
                    log.fine("Stopping datagram I/O on address: " + entry.getKey());
                    entry.getValue().stop();
                }
                datagramIOs.clear();

                networkAddressFactory = null;
                enabled = false;
                return true;
            }
            return false;
        } finally {
            unlock(writeLock);
        }
    }

    @Override
    public void shutdown() throws RouterException {
        disable();
    }

    @Override
    public boolean isEnabled() {
        return enabled;
    }

    @Override
    public void handleStartFailure(InitializationException ex) throws InitializationException {
        if (ex instanceof NoNetworkException) {
            log.info("Unable to initialize network router, no network found.");
        } else {
            log.severe("Unable to initialize network router: " + ex);
            log.severe("Cause: " + Exceptions.unwrap(ex));
        }
    }

    public List<NetworkAddress> getActiveStreamServers(InetAddress preferredAddress) throws RouterException {
        lock(readLock);
        try {
            if (enabled && streamServers.size() > 0) {
                List<NetworkAddress> streamServerAddresses = new ArrayList<>();

                StreamServer preferredServer;
                if (preferredAddress != null &&
                    (preferredServer = streamServers.get(preferredAddress)) != null) {
                    streamServerAddresses.add(
                        new NetworkAddress(
                            preferredAddress,
                            preferredServer.getPort(),
                            networkAddressFactory.getHardwareAddress(preferredAddress)

                        )
                    );
                    return streamServerAddresses;
                }

                for (Map.Entry<InetAddress, StreamServer> entry : streamServers.entrySet()) {
                    byte[] hardwareAddress = networkAddressFactory.getHardwareAddress(entry.getKey());
                    streamServerAddresses.add(
                        new NetworkAddress(entry.getKey(), entry.getValue().getPort(), hardwareAddress)
                    );
                }
                return streamServerAddresses;
            } else {
                return Collections.EMPTY_LIST;
            }
        } finally {
            unlock(readLock);
        }
    }

    /**
     * Obtains the asynchronous protocol {@code Executor} and runs the protocol created
     * by the {@link org.fourthline.cling.protocol.ProtocolFactory} for the given message.
     * <p>
     * If the factory doesn't create a protocol, the message is dropped immediately without
     * creating another thread or consuming further resources. This means we can filter the
     * datagrams in the protocol factory and e.g. completely disable discovery or only
     * allow notification message from some known services we'd like to work with.
     * </p>
     *
     * @param msg The received datagram message.
     */
    public void received(IncomingDatagramMessage msg) {
        if (!enabled) {
            log.fine("Router disabled, ignoring incoming message: " + msg);
            return;
        }
        try {
            ReceivingAsync protocol = getProtocolFactory().createReceivingAsync(msg);
            if (protocol == null) {
                if (log.isLoggable(Level.FINEST))
                    log.finest("No protocol, ignoring received message: " + msg);
                return;
            }
            if (log.isLoggable(Level.FINE))
                log.fine("Received asynchronous message: " + msg);
            getConfiguration().getAsyncProtocolExecutor().execute(protocol);
        } catch (ProtocolCreationException ex) {
            log.warning("Handling received datagram failed - " + Exceptions.unwrap(ex).toString());
        }
    }

    /**
     * Obtains the synchronous protocol {@code Executor} and runs the
     * {@link org.fourthline.cling.transport.spi.UpnpStream} directly.
     *
     * @param stream The received {@link org.fourthline.cling.transport.spi.UpnpStream}.
     */
    public void received(UpnpStream stream) {
        if (!enabled) {
            log.fine("Router disabled, ignoring incoming: " + stream);
            return;
        }
        log.fine("Received synchronous stream: " + stream);
        getConfiguration().getSyncProtocolExecutorService().execute(stream);
    }

    /**
     * Sends the UDP datagram on all bound {@link org.fourthline.cling.transport.spi.DatagramIO}s.
     * <p>
     * The transport set is copied while the router lock is held, the datagrams are written after
     * it has been released (see the locking rule on this class).
     * </p>
     *
     * @param msg The UDP datagram message to send.
     */
    public void send(OutgoingDatagramMessage msg) throws RouterException {

        Map<InetAddress, DatagramIO> transports;

        try {
            lock(readLock);
            try {
                if (!enabled) {
                    log.fine("Router disabled, not sending datagram: " + msg);
                    return;
                }
                transports = new LinkedHashMap<>(datagramIOs);
            } finally {
                unlock(readLock);
            }
        } catch (RouterException ex) {
            // Router is busy being enabled/disabled, drop the datagram - SSDP is lossy by nature
            // and this must never fail the (synchronous) protocol which sent it.
            log.info("RouterImpl send error : " + ex);
            return;
        }

        for (DatagramIO datagramIO : transports.values()) {
            try {
                datagramIO.send(msg);
            } catch (Exception exception) {
                // One broken transport must not prevent the message from going out on the others
                log.info("RouterImpl send error : " + exception.toString());
            }
        }
    }

    /**
     * Sends the TCP stream request with the {@link org.fourthline.cling.transport.spi.StreamClient}.
     * <p>
     * The client reference is taken while the router lock is held, the actual request is sent
     * after it has been released. This is essential: a request to an unreachable device blocks
     * for up to <code>getTimeoutSeconds()</code> (60 seconds by default), and holding the read
     * lock for that long makes every {@link #disable()}/{@link #enable()} - and on Android the
     * main thread - wait, which is an ANR.
     * </p>
     *
     * @param msg The TCP (HTTP) stream message to send.
     * @return The return value of the {@link org.fourthline.cling.transport.spi.StreamClient#sendRequest(StreamRequestMessage)}
     *         method or <code>null</code> if no <code>StreamClient</code> is available.
     */
    public StreamResponseMessage send(StreamRequestMessage msg) throws RouterException {

        StreamClient client;
        boolean routerEnabled;

        lock(readLock);
        try {
            routerEnabled = enabled;
            client = streamClient;
        } finally {
            unlock(readLock);
        }

        if (!routerEnabled) {
            log.fine("Router disabled, not sending stream request: " + msg);
            return null;
        }
        if (client == null) {
            log.fine("No StreamClient available, not sending: " + msg);
            return null;
        }

        log.fine("Sending via TCP unicast stream: " + msg);
        try {
            return client.sendRequest(msg);
        } catch (InterruptedException ex) {
            throw new RouterException("Sending stream request was interrupted", ex);
        } catch (RuntimeException ex) {
            // The client can be stopped concurrently by disable(), e.g. a rejected execution
            throw new RouterException("Sending stream request failed: " + ex, ex);
        }
    }

    /**
     * Sends the given bytes as a broadcast on all bound {@link org.fourthline.cling.transport.spi.DatagramIO}s,
     * using source port 9.
     * <p>
     * TODO: Support source port parameter
     * </p>
     *
     * @param bytes The byte payload of the UDP datagram.
     */
    public void broadcast(byte[] bytes) throws RouterException {

        Map<InetAddress, DatagramIO> transports;
        NetworkAddressFactory addresses;

        lock(readLock);
        try {
            if (!enabled) {
                log.fine("Router disabled, not broadcasting bytes: " + bytes.length);
                return;
            }
            transports = new LinkedHashMap<>(datagramIOs);
            addresses = networkAddressFactory;
        } finally {
            unlock(readLock);
        }

        if (addresses == null) {
            // Disabled in the meantime
            return;
        }

        for (Map.Entry<InetAddress, DatagramIO> entry : transports.entrySet()) {
            InetAddress broadcast = addresses.getBroadcastAddress(entry.getKey());
            if (broadcast != null) {
                log.fine("Sending UDP datagram to broadcast address: " + broadcast.getHostAddress());
                DatagramPacket packet = new DatagramPacket(bytes, bytes.length, broadcast, 9);
                try {
                    entry.getValue().send(packet);
                } catch (Exception exception) {
                    log.info("RouterImpl broadcast error : " + exception.toString());
                }
            }
        }
    }

    protected void startInterfaceBasedTransports(Iterator<NetworkInterface> interfaces) throws InitializationException {
        while (interfaces.hasNext()) {
            NetworkInterface networkInterface = interfaces.next();

            // We only have the MulticastReceiver as an interface-based transport
            MulticastReceiver multicastReceiver = getConfiguration().createMulticastReceiver(networkAddressFactory);
            if (multicastReceiver == null) {
                log.info("Configuration did not create a MulticastReceiver for: " + networkInterface);
            } else {
                try {
                    if (log.isLoggable(Level.FINE))
                        log.fine("Init multicast receiver on interface: " + networkInterface.getDisplayName());
                    multicastReceiver.init(
                        networkInterface,
                        this,
                        networkAddressFactory,
                        getConfiguration().getDatagramProcessor()
                    );

                    multicastReceivers.put(networkInterface, multicastReceiver);
                } catch (InitializationException ex) {
                    /* TODO: What are some recoverable exceptions for this?
                    log.warning(
                        "Ignoring network interface '"
                            + networkInterface.getDisplayName()
                            + "' init failure of MulticastReceiver: " + ex.toString());
                    if (log.isLoggable(Level.FINE))
                        log.log(Level.FINE, "Initialization exception root cause", Exceptions.unwrap(ex));
                    log.warning("Removing unusable interface " + interface);
                    it.remove();
                    continue; // Don't need to try anything else on this interface
                    */
                    throw ex;
                }
            }
        }

        for (Map.Entry<NetworkInterface, MulticastReceiver> entry : multicastReceivers.entrySet()) {
            if (log.isLoggable(Level.FINE))
                log.fine("Starting multicast receiver on interface: " + entry.getKey().getDisplayName());
            getConfiguration().getMulticastReceiverExecutor().execute(entry.getValue());
        }
    }

    protected void startAddressBasedTransports(Iterator<InetAddress> addresses) throws InitializationException {
        while (addresses.hasNext()) {
            InetAddress address = addresses.next();

            // HTTP servers
            StreamServer streamServer = getConfiguration().createStreamServer(networkAddressFactory);
            if (streamServer == null) {
                log.info("Configuration did not create a StreamServer for: " + address);
            } else {
                try {
                    if (log.isLoggable(Level.FINE))
                        log.fine("Init stream server on address: " + address);
                    streamServer.init(address, this);
                    streamServers.put(address, streamServer);
                } catch (InitializationException ex) {
                    // Try to recover
                    Throwable cause = Exceptions.unwrap(ex);
                    if (cause instanceof BindException) {
                        log.warning("Failed to init StreamServer: " + cause);
                        if (log.isLoggable(Level.FINE))
                            log.log(Level.FINE, "Initialization exception root cause", cause);
                        log.warning("Removing unusable address: " + address);
                        addresses.remove();
                        continue; // Don't try anything else with this address
                    }
                    throw ex;
                }
            }

            // Datagram I/O
            DatagramIO datagramIO = getConfiguration().createDatagramIO(networkAddressFactory);
            if (datagramIO == null) {
                log.info("Configuration did not create a StreamServer for: " + address);
            } else {
                try {
                    if (log.isLoggable(Level.FINE))
                        log.fine("Init datagram I/O on address: " + address);
                    datagramIO.init(address, this, getConfiguration().getDatagramProcessor());
                    datagramIOs.put(address, datagramIO);
                } catch (InitializationException ex) {
                    /* TODO: What are some recoverable exceptions for this?
                    Throwable cause = Exceptions.unwrap(ex);
                    if (cause instanceof BindException) {
                        log.warning("Failed to init datagram I/O: " + cause);
                        if (log.isLoggable(Level.FINE))
                            log.log(Level.FINE, "Initialization exception root cause", cause);
                        log.warning("Removing unusable address: " + address);
                        addresses.remove();
                        continue; // Don't try anything else with this address
                    }
                    */
                    throw ex;
                }
            }
        }

        for (Map.Entry<InetAddress, StreamServer> entry : streamServers.entrySet()) {
            if (log.isLoggable(Level.FINE))
                log.fine("Starting stream server on address: " + entry.getKey());
            getConfiguration().getStreamServerExecutorService().execute(entry.getValue());
        }

        for (Map.Entry<InetAddress, DatagramIO> entry : datagramIOs.entrySet()) {
            if (log.isLoggable(Level.FINE))
                log.fine("Starting datagram I/O on address: " + entry.getKey());
            getConfiguration().getDatagramIOExecutor().execute(entry.getValue());
        }
    }

    protected void lock(Lock lock, int timeoutMilliseconds) throws RouterException {
        try {
            log.finest("Trying to obtain lock with timeout milliseconds '" + timeoutMilliseconds + "': " + lock.getClass().getSimpleName());
            if (lock.tryLock(timeoutMilliseconds, TimeUnit.MILLISECONDS)) {
                log.finest("Acquired router lock: " + lock.getClass().getSimpleName());
            } else {
                throw new RouterException(
                    "Router wasn't available exclusively after waiting " + timeoutMilliseconds + "ms, lock failed: "
                        + lock.getClass().getSimpleName()
                );
            }
        } catch (InterruptedException ex) {
            throw new RouterException(
                "Interruption while waiting for exclusive access: " + lock.getClass().getSimpleName(), ex
            );
        }
    }

    protected void lock(Lock lock) throws RouterException {
        lock(lock, getLockTimeoutMillis());
    }

    protected void unlock(Lock lock) {
        try{
            log.finest("Releasing router lock: " + lock.getClass().getSimpleName());
            lock.unlock();
        }catch(Exception e){
            log.fine("RouterImpl unlock error : " + e.toString());
        }
        
    }

    /**
     * @return Defaults to 6 seconds, should be longer than it takes the router to be enabled/disabled.
     */
    protected int getLockTimeoutMillis() {
        return 6000;
    }

}
