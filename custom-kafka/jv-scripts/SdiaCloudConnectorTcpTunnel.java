/*
 * ============================================================================
 * Event Smart Kafka Adapter
 * SDIA — Semantic Domain Integration Architecture
 * ============================================================================
 *
 * Copyright (c) 2026 Ricardo Luz Holanda Viana
 * Independent Solo Researcher | Enterprise Integration Architecture
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * ============================================================================
 * SOCKS5 Proxy Support — Original Implementation
 * ============================================================================
 *
 * This SOCKS5 implementation was created from scratch by Ricardo Luz Holanda Viana
 * as part of the SDIA project. It is provided free of charge under the terms
 * of the Apache License 2.0.
 *
 * NOTICE: This file header must be preserved in all copies, modifications,
 * or distributions of this file, whether in source or binary form.
 * Removal or alteration of this copyright notice is not permitted.
 *
 * This notice is consistent with the attribution requirements of the
 * Apache License 2.0 (Section 4.4) and does not impose additional
 * restrictions beyond those already required by the License.
 *
 * ============================================================================
 */
package custom.opensource.cpi.sdia.smart.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * High-performance local TCP relay for SAP Cloud Connector TCP access.
 *
 * Why this exists:
 * - Apache Kafka Java Client does not expose a stable hook to provide a custom SOCKS5 socket.
 * - SAP Cloud Connector TCP access is exposed through SAP Connectivity SOCKS5 proxy.
 * - This tunnel keeps Kafka untouched: Kafka connects to 127.0.0.1:<brokerPort>, while this
 *   relay forwards the raw TCP stream to the Cloud Connector virtual host through SOCKS5.
 *
 * CPI/ADK usage:
 * - Only start this tunnel when Proxy Type = On-Premise.
 * - For Internet/Cloud brokers, do not instantiate this class.
 *
 * Kafka requirement:
 * - The external/client advertised listener must be 127.0.0.1:<samePort> or localhost:<samePort>,
 *   because Kafka metadata will point the Kafka client back to the local relay.
 */
public final class SdiaCloudConnectorTcpTunnel implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(SdiaCloudConnectorTcpTunnel.class);

    private static final String LOCAL_BIND_HOST = "127.0.0.1";
    private static final int CONNECT_TIMEOUT_MS = 10000;
    private static final int HANDSHAKE_TIMEOUT_MS = 15000;
    private static final int PIPE_BUFFER_SIZE = 65536; // matches socket SO_RCVBUF/SO_SNDBUF (65536) below
    private static final int SERVER_BACKLOG = 64;
    private static final int MAX_RELAY_THREADS = 256;
    private static final int MAX_ACTIVE_SOCKETS_PER_RELAY = 512;
    private static final AtomicInteger RELAY_THREAD_SEQUENCE = new AtomicInteger(1);

    private static final ExecutorService RELAY_EXECUTOR = new ThreadPoolExecutor(
            0,
            MAX_RELAY_THREADS,
            60L,
            TimeUnit.SECONDS,
            new SynchronousQueue<Runnable>(),
            new ThreadFactory() {
                @Override
                public Thread newThread(final Runnable r) {
                    final Thread t = new Thread(r, "sdia-cc-kafka-relay-" + RELAY_THREAD_SEQUENCE.getAndIncrement());
                    t.setDaemon(true);
                    return t;
                }
            },
            new ThreadPoolExecutor.AbortPolicy());

    private static final Map<String, RelayLease> ACTIVE_RELAYS = new ConcurrentHashMap<String, RelayLease>();
    private static final Object ACTIVE_RELAYS_LOCK = new Object();

    private final TunnelConfig config;
    private final List<TunnelServer> servers = new ArrayList<TunnelServer>(4);
    private final AtomicBoolean running = new AtomicBoolean(false);

    public SdiaCloudConnectorTcpTunnel(final TunnelConfig config) {
        if (config == null) {
            throw new IllegalArgumentException("TunnelConfig must not be null.");
        }
        if (config.proxyConfig == null) {
            throw new IllegalArgumentException("TunnelConfig.proxyConfig must not be null.");
        }
        if (config.targets == null || config.targets.isEmpty()) {
            throw new IllegalArgumentException("TunnelConfig.targets must not be empty.");
        }
        this.config = config;
    }

    /**
     * Starts one local listener per configured Kafka bootstrap port.
     * This method is intentionally fail-fast: if the SCC SOCKS5 CONNECT fails, Kafka is not started.
     */
    public synchronized void start() throws IOException {
        if (running.get()) {
            return;
        }

        final List<TunnelServer> started = new ArrayList<TunnelServer>(config.targets.size());
        try {
            for (BootstrapAddress target : config.targets) {
                final String relayKey = relayKey(config.proxyConfig, target, config.localBindHost);
                RelayLease retainedLease = retainRegisteredRelay(relayKey);
                TunnelServer server;

                if (retainedLease != null) {
                    server = TunnelServer.passive(target, config.localBindHost, retainedLease);
                    LOG.warn("[SDIA Kafka] SAP CC TCP relay already registered in this OSGi worker. Reusing it with ref-count. local="
                            + config.localBindHost + ':' + target.port
                            + " -> virtual=" + target.authority());
                    started.add(server);
                    continue;
                }

                testProxyConnect(config.proxyConfig, target);

                try {
                    server = new TunnelServer(config.proxyConfig, target, config.localBindHost,
                            newActiveSocketSet(), relayKey);
                    server.start();

                    RelayLease raceLease = registerOwnedRelayOrReturnExisting(relayKey, server);
                    if (raceLease != null) {
                        server.closePhysical();
                        server = TunnelServer.passive(target, config.localBindHost, raceLease);
                        LOG.warn("[SDIA Kafka] SAP CC TCP relay was started concurrently by another iFlow. "
                                + "Closed duplicate local listener and reused registered relay. local="
                                + config.localBindHost + ':' + target.port + " -> virtual=" + target.authority());
                    } else {
                        LOG.warn("[SDIA Kafka] SAP CC TCP relay started. local=" + server.localBootstrap()
                                + " -> virtual=" + target.authority()
                                + " via socks=" + config.proxyConfig.maskedAuthority()
                                + " | auth=" + config.proxyConfig.authDescription()
                                + " | locationId=" + displayOptionalLocationId(config.proxyConfig.locationId));
                    }
                } catch (IOException bindEx) {
                    // Do not passively reuse a local listener that is not visible in ACTIVE_RELAYS.
                    // A port can be open because an older classloader or a broken undeploy left a
                    // zombie relay behind. A plain TCP connect only proves that something accepts
                    // the socket; it does not prove that the relay still forwards Kafka traffic
                    // through SAP Cloud Connector. Reusing it turns a real lifecycle problem into
                    // misleading Kafka metadata timeouts. Fail fast instead.
                    if (isAddressAlreadyInUse(bindEx)) {
                        final boolean reachable = isLocalRelayReachable(config.localBindHost, target.port);
                        throw new IOException("SAP CC TCP relay local port is already in use but not registered "
                                + "in the current OSGi relay registry. Refusing unsafe passive reuse to avoid "
                                + "zombie relay / false Kafka metadata timeout. local="
                                + config.localBindHost + ':' + target.port
                                + " virtual=" + target.authority()
                                + " reachable=" + reachable
                                + ". Stop the old channel/runtime that owns this port or choose a clean relay port.",
                                bindEx);
                    }
                    throw bindEx;
                }
                started.add(server);
            }
            servers.addAll(started);
            running.set(true);
        } catch (Throwable startEx) {
            for (TunnelServer server : started) {
                try { server.close(); } catch (Throwable ignored) {}
            }
            if (startEx instanceof IOException) {
                throw (IOException) startEx;
            }
            throw new IOException("SAP Cloud Connector TCP relay startup failed: " + rootCauseMessage(startEx), startEx);
        }
    }

    public boolean isRunning() {
        return running.get();
    }

    public String getLocalBootstrapServers() {
        final StringBuilder sb = new StringBuilder(128);
        if (servers.isEmpty()) {
            for (int i = 0; i < config.targets.size(); i++) {
                if (i > 0) sb.append(',');
                sb.append(config.localBindHost).append(':').append(config.targets.get(i).port);
            }
            return sb.toString();
        }
        for (int i = 0; i < servers.size(); i++) {
            if (i > 0) sb.append(',');
            sb.append(servers.get(i).localBootstrap());
        }
        return sb.toString();
    }

    @Override
    public synchronized void close() {
        running.set(false);
        for (TunnelServer server : servers) {
            try { server.close(); } catch (Throwable ignored) {}
        }
        servers.clear();
    }

    private static Set<Socket> newActiveSocketSet() {
        return Collections.newSetFromMap(new ConcurrentHashMap<Socket, Boolean>());
    }

    private static String relayKey(final ProxyConfig proxyConfig,
                                   final BootstrapAddress target,
                                   final String localBindHost) {
        final StringBuilder key = new StringBuilder(192);
        key.append(safeTrim(localBindHost) == null ? LOCAL_BIND_HOST : localBindHost.trim());
        key.append(':').append(target.port);
        key.append("|target=").append(target.authority());
        key.append("|proxy=").append(proxyConfig.proxyHost).append(':').append(proxyConfig.proxyPort);
        key.append("|location=").append(safeTrim(proxyConfig.locationId) == null ? "" : proxyConfig.locationId.trim());
        return key.toString();
    }

    private static RelayLease retainRegisteredRelay(final String relayKey) {
        synchronized (ACTIVE_RELAYS_LOCK) {
            final RelayLease lease = ACTIVE_RELAYS.get(relayKey);
            if (lease == null) {
                return null;
            }
            if (lease.retainIfRunning()) {
                return lease;
            }
            ACTIVE_RELAYS.remove(relayKey, lease);
            return null;
        }
    }

    /**
     * Registers a newly-created owned relay. Returns an existing retained lease when
     * another iFlow won the race while this caller was binding its local ServerSocket.
     */
    private static RelayLease registerOwnedRelayOrReturnExisting(final String relayKey,
                                                                 final TunnelServer server) {
        synchronized (ACTIVE_RELAYS_LOCK) {
            final RelayLease existing = ACTIVE_RELAYS.get(relayKey);
            if (existing != null && existing.retainIfRunning()) {
                return existing;
            }
            if (existing != null) {
                ACTIVE_RELAYS.remove(relayKey, existing);
            }
            final RelayLease created = new RelayLease(relayKey, server);
            server.attachLease(created);
            ACTIVE_RELAYS.put(relayKey, created);
            return null;
        }
    }

    private static boolean isAddressAlreadyInUse(final Throwable t) {
        Throwable c = t;
        while (c != null) {
            if (c instanceof java.net.BindException) {
                return true;
            }
            final String m = c.getMessage();
            if (m != null && m.toLowerCase().indexOf("address already in use") >= 0) {
                return true;
            }
            c = c.getCause();
        }
        return false;
    }

    // Confirms a live relay already listens on host:port before reusing it passively.
    // Retried a few times because a sibling iFlow's relay may be a few ms from being ready.
    private static boolean isLocalRelayReachable(final String host, final int port) {
        final String h = (safeTrim(host) == null) ? "127.0.0.1" : host.trim();
        for (int attempt = 0; attempt < 3; attempt++) {
            Socket s = null;
            try {
                s = new Socket();
                s.connect(new InetSocketAddress(InetAddress.getByName(h), port), 2000);
                return true;
            } catch (Throwable ignored) {
                // not reachable yet
            } finally {
                if (s != null) {
                    try { s.close(); } catch (Throwable ignored) {}
                }
            }
            try {
                Thread.sleep(300L);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
        return false;
    }

    static void testProxyConnect(final ProxyConfig proxyConfig,
                                 final BootstrapAddress target) throws IOException {
        Socket socket = null;
        try {
            socket = openConnectedProxySocket(proxyConfig, target, true);
        } finally {
            closeSocket(socket);
        }
    }

    static Socket openConnectedProxySocket(final ProxyConfig proxyConfig,
                                           final BootstrapAddress target,
                                           final boolean probeMode) throws IOException {
        final Socket socket = new Socket();
        boolean connected = false;
        try {
            socket.setTcpNoDelay(true);
            socket.setKeepAlive(true);
            try { socket.setReceiveBufferSize(65536); } catch (Throwable ignored) {}
            try { socket.setSendBufferSize(65536); } catch (Throwable ignored) {}

            socket.connect(new InetSocketAddress(proxyConfig.proxyHost, proxyConfig.proxyPort), CONNECT_TIMEOUT_MS);
            socket.setSoTimeout(HANDSHAKE_TIMEOUT_MS);
            performSocks5Handshake(socket, proxyConfig, target);
            socket.setSoTimeout(0);

            if (probeMode) {
                LOG.warn("[SDIA Kafka] SAP CC SOCKS5 probe OK. target=" + target.authority()
                        + " | proxy=" + proxyConfig.maskedAuthority()
                        + " | auth=" + proxyConfig.authDescription()
                        + " | locationId=" + displayOptionalLocationId(proxyConfig.locationId));
            }

            connected = true;
            return socket;
        } finally {
            if (!connected) {
                closeSocket(socket);
            }
        }
    }

    private static void performSocks5Handshake(final Socket socket,
                                               final ProxyConfig proxyConfig,
                                               final BootstrapAddress target) throws IOException {
        final InputStream in = socket.getInputStream();
        final OutputStream out = socket.getOutputStream();

        final boolean offerTokenAuth = proxyConfig.shouldOfferTokenAuth();
        writeGreeting(out, offerTokenAuth);

        final byte[] greeting = readFully(in, 2, "SOCKS5 method-selection response");
        if ((greeting[0] & 0xFF) != 0x05) {
            throw new IOException("Invalid SOCKS5 response version from SAP Connectivity proxy: 0x" + toHex(greeting[0]));
        }

        final int method = greeting[1] & 0xFF;
        if (method == 0xFF) {
            throw new IOException("SAP Connectivity SOCKS5 proxy rejected all authentication methods. Offered="
                    + proxyConfig.offeredAuthMethodsDescription(offerTokenAuth));
        }

        if (method == 0x80) {
            performSapJwtSocks5Auth(in, out, proxyConfig);
        } else if (method == 0x02) {
            performUsernamePasswordAuth(in, out, proxyConfig);
        } else if (method == 0x00) {
            // No-auth accepted by the runtime/proxy. Valid in some local/test topologies.
        } else {
            throw new IOException("SAP Connectivity SOCKS5 proxy selected unsupported auth method: 0x"
                    + toHex((byte) method) + ". Offered=" + proxyConfig.offeredAuthMethodsDescription(offerTokenAuth));
        }

        writeConnectRequest(out, target);
        readConnectResponse(in, target);
    }

    private static void writeGreeting(final OutputStream out,
                                      final boolean offerTokenAuth) throws IOException {
        if (offerTokenAuth) {
            // Offer SAP private JWT auth first, then RFC1929 username/password fallback, then no-auth.
            out.write(new byte[] { 0x05, 0x03, (byte) 0x80, 0x02, 0x00 });
        } else {
            out.write(new byte[] { 0x05, 0x01, 0x00 });
        }
        out.flush();
    }

    /**
     * SAP Connectivity SOCKS5 private authentication method 0x80.
     * Payload: version(0x01) + jwtLength(4 bytes BE) + jwt + locationIdLength(1 byte) + base64(locationId).
     */
    private static void performSapJwtSocks5Auth(final InputStream in,
                                                final OutputStream out,
                                                final ProxyConfig proxyConfig) throws IOException {
        final String token = safeTrim(proxyConfig.resolveCurrentBearerToken());
        if (token == null) {
            throw new IOException("SAP Connectivity selected SOCKS5 JWT auth 0x80, but no bearer token is available.");
        }

        final byte[] jwt = token.getBytes(StandardCharsets.UTF_8);
        final byte[] encodedLocation = encodeLocationId(proxyConfig.locationId);

        out.write(0x01); // SAP auth sub-negotiation version
        out.write((jwt.length >> 24) & 0xFF);
        out.write((jwt.length >> 16) & 0xFF);
        out.write((jwt.length >> 8) & 0xFF);
        out.write(jwt.length & 0xFF);
        out.write(jwt);
        out.write(encodedLocation.length & 0xFF);
        if (encodedLocation.length > 0) {
            out.write(encodedLocation);
        }
        out.flush();

        final byte[] response = readFully(in, 2, "SAP SOCKS5 JWT auth response");
        if ((response[0] & 0xFF) != 0x01 || (response[1] & 0xFF) != 0x00) {
            throw new IOException("SAP Connectivity SOCKS5 JWT authentication failed. Response version=0x"
                    + toHex(response[0]) + " status=0x" + toHex(response[1])
                    + ". Location ID is optional; set it only when the SCC subaccount connection uses one.");
        }
    }

    /**
     * Conservative fallback for runtimes exposing RFC1929 SOCKS5 username/password auth.
     * Username = bearer token, password = raw Location ID or empty.
     */
    private static void performUsernamePasswordAuth(final InputStream in,
                                                    final OutputStream out,
                                                    final ProxyConfig proxyConfig) throws IOException {
        final String token = safeTrim(proxyConfig.resolveCurrentBearerToken());
        if (token == null) {
            throw new IOException("SOCKS5 username/password selected, but bearer token is unavailable.");
        }
        final byte[] username = token.getBytes(StandardCharsets.UTF_8);
        final String loc = safeTrim(proxyConfig.locationId);
        final byte[] password = (loc == null ? "" : loc).getBytes(StandardCharsets.UTF_8);
        if (username.length > 255) {
            throw new IOException("SOCKS5 username/password fallback cannot send bearer token > 255 bytes.");
        }
        if (password.length > 255) {
            throw new IOException("SOCKS5 username/password fallback cannot send Location ID > 255 bytes.");
        }
        out.write(0x01); // RFC1929 version
        out.write(username.length & 0xFF);
        out.write(username);
        out.write(password.length & 0xFF);
        if (password.length > 0) {
            out.write(password);
        }
        out.flush();

        final byte[] response = readFully(in, 2, "SOCKS5 username/password auth response");
        if ((response[0] & 0xFF) != 0x01 || (response[1] & 0xFF) != 0x00) {
            throw new IOException("SOCKS5 username/password authentication failed. Response version=0x"
                    + toHex(response[0]) + " status=0x" + toHex(response[1]));
        }
    }

    private static void writeConnectRequest(final OutputStream out,
                                            final BootstrapAddress target) throws IOException {
        final byte[] host = target.host.getBytes(StandardCharsets.UTF_8);
        if (host.length == 0 || host.length > 255) {
            throw new IOException("Invalid SOCKS5 target host length for Cloud Connector virtual host: " + target.host);
        }
        out.write(0x05); // VER
        out.write(0x01); // CONNECT
        out.write(0x00); // RSV
        out.write(0x03); // DOMAINNAME: keep virtual host unresolved locally
        out.write(host.length & 0xFF);
        out.write(host);
        out.write((target.port >> 8) & 0xFF);
        out.write(target.port & 0xFF);
        out.flush();
    }

    private static void readConnectResponse(final InputStream in,
                                            final BootstrapAddress target) throws IOException {
        final byte[] header = readFully(in, 4, "SOCKS5 CONNECT response header");
        if ((header[0] & 0xFF) != 0x05) {
            throw new IOException("Invalid SOCKS5 CONNECT response version from SAP Connectivity proxy: 0x" + toHex(header[0]));
        }
        final int rep = header[1] & 0xFF;
        if (rep != 0x00) {
            throw new IOException("SAP Connectivity SOCKS5 CONNECT failed for target "
                    + target.authority() + ". REP=0x" + toHex((byte) rep) + " (" + socks5RepMessage(rep) + ")");
        }
        final int atyp = header[3] & 0xFF;
        if (atyp == 0x01) {
            readFully(in, 4, "SOCKS5 IPv4 bind address");
        } else if (atyp == 0x03) {
            final byte[] len = readFully(in, 1, "SOCKS5 domain bind address length");
            readFully(in, len[0] & 0xFF, "SOCKS5 domain bind address");
        } else if (atyp == 0x04) {
            readFully(in, 16, "SOCKS5 IPv6 bind address");
        } else {
            throw new IOException("Unsupported SOCKS5 bind address type from SAP Connectivity proxy: 0x" + toHex((byte) atyp));
        }
        readFully(in, 2, "SOCKS5 bind port");
    }

    private static byte[] encodeLocationId(final String locationId) throws IOException {
        final String loc = safeTrim(locationId);
        if (loc == null) {
            return new byte[0];
        }
        final byte[] encoded = java.util.Base64.getEncoder().encode(loc.getBytes(StandardCharsets.UTF_8));
        if (encoded.length > 255) {
            throw new IOException("Cloud Connector Location ID is too long after Base64 encoding. Length=" + encoded.length);
        }
        return encoded;
    }

    private static byte[] readFully(final InputStream in,
                                    final int len,
                                    final String stage) throws IOException {
        final byte[] data = new byte[len];
        int off = 0;
        while (off < len) {
            final int read = in.read(data, off, len - off);
            if (read < 0) {
                throw new IOException("Unexpected end of stream while reading " + stage + ".");
            }
            off += read;
        }
        return data;
    }

    private static final class TunnelServer implements AutoCloseable, Runnable {
        private final ProxyConfig proxyConfig;
        private final BootstrapAddress target;
        private final ServerSocket serverSocket;
        private final Thread acceptThread;
        private final AtomicBoolean running = new AtomicBoolean(true);
        private final String localBindHost;
        private final boolean passive;
        private final Set<Socket> activeSockets;
        private final String relayKey;
        private final AtomicBoolean closeRequested = new AtomicBoolean(false);
        private RelayLease lease;

        TunnelServer(final ProxyConfig proxyConfig,
                     final BootstrapAddress target,
                     final String localBindHost,
                     final Set<Socket> activeSockets,
                     final String relayKey) throws IOException {
            this.proxyConfig = proxyConfig;
            this.target = target;
            this.localBindHost = localBindHost;
            this.activeSockets = activeSockets;
            this.relayKey = relayKey;
            this.serverSocket = new ServerSocket();
            this.serverSocket.setReuseAddress(true);
            this.serverSocket.bind(new InetSocketAddress(InetAddress.getByName(localBindHost), target.port), SERVER_BACKLOG);
            this.acceptThread = new Thread(this, "sdia-cc-kafka-tcp-accept-" + target.port);
            this.acceptThread.setDaemon(true);
            this.passive = false;
        }

        // Passive reuse: another iFlow in this CPI runtime already owns the shared relay on
        // localBindHost:target.port. This instance owns no socket and no thread; the Kafka client
        // connects to the existing relay. close() must NOT touch the shared relay it does not own.
        private TunnelServer(final BootstrapAddress target,
                             final String localBindHost,
                             final RelayLease lease) {
            this.proxyConfig = null;
            this.target = target;
            this.localBindHost = localBindHost;
            this.serverSocket = null;
            this.acceptThread = null;
            this.activeSockets = Collections.emptySet();
            this.relayKey = null;
            this.lease = lease;
            this.passive = true;
        }

        static TunnelServer passive(final BootstrapAddress target,
                                    final String localBindHost,
                                    final RelayLease lease) {
            return new TunnelServer(target, localBindHost, lease);
        }

        void attachLease(final RelayLease lease) {
            this.lease = lease;
        }

        void start() {
            if (passive || acceptThread == null) {
                return;
            }
            this.acceptThread.start();
        }

        String localBootstrap() {
            return localBindHost + ':' + target.port;
        }

        @Override
        public void run() {
            while (running.get()) {
                Socket clientSocket = null;
                try {
                    clientSocket = serverSocket.accept();
                    configureDataSocket(clientSocket);
                    if (activeSockets.size() >= MAX_ACTIVE_SOCKETS_PER_RELAY) {
                        LOG.warn("[SDIA Kafka] CloudConnector relay active socket limit reached. Closing Kafka relay socket. "
                                + "target=" + target.authority()
                                + " activeSockets=" + activeSockets.size()
                                + " maxActiveSocketsPerRelay=" + MAX_ACTIVE_SOCKETS_PER_RELAY);
                        closeSocket(clientSocket);
                        clientSocket = null;
                        continue;
                    }
                    activeSockets.add(clientSocket);
                    try {
                        RELAY_EXECUTOR.execute(new TunnelConnection(proxyConfig, target, clientSocket, activeSockets));
                    } catch (RejectedExecutionException rejected) {
                        LOG.warn("[SDIA Kafka] CloudConnector relay thread pool saturated. Closing Kafka relay socket. "
                                + "target=" + target.authority()
                                + " maxRelayThreads=" + MAX_RELAY_THREADS);
                        closeSocket(clientSocket);
                        activeSockets.remove(clientSocket);
                        clientSocket = null;
                    }
                } catch (Throwable acceptEx) {
                    closeSocket(clientSocket);
                    if (clientSocket != null) activeSockets.remove(clientSocket);
                    if (running.get()) {
                        LOG.warn("[SDIA Kafka] SAP CC TCP relay accept failed for target "
                                + target.authority() + ": " + rootCauseMessage(acceptEx));
                    }
                }
            }
        }

        @Override
        public void close() {
            if (!closeRequested.compareAndSet(false, true)) {
                return;
            }
            final RelayLease currentLease = this.lease;
            if (currentLease != null) {
                currentLease.release();
                return;
            }
            closePhysical();
        }

        boolean isAccepting() {
            return !passive && running.get() && serverSocket != null && !serverSocket.isClosed();
        }

        void closePhysical() {
            if (passive) {
                return;
            }
            running.set(false);
            try { serverSocket.close(); } catch (Throwable ignored) {}
            for (Socket socket : activeSockets) {
                closeSocket(socket);
            }
            activeSockets.clear();
        }
    }

    private static final class RelayLease {
        private final String relayKey;
        private final TunnelServer owner;
        private final AtomicInteger refCount = new AtomicInteger(1);
        private final AtomicBoolean closed = new AtomicBoolean(false);

        RelayLease(final String relayKey, final TunnelServer owner) {
            this.relayKey = relayKey;
            this.owner = owner;
        }

        boolean retainIfRunning() {
            // Must be called while holding ACTIVE_RELAYS_LOCK. This keeps retain/release
            // linearized and prevents a redeploying channel from retaining a relay while
            // the last previous channel is concurrently closing its physical ServerSocket.
            if (closed.get() || owner == null || !owner.isAccepting()) {
                return false;
            }
            refCount.incrementAndGet();
            if (closed.get() || !owner.isAccepting()) {
                release();
                return false;
            }
            return true;
        }

        void release() {
            boolean closeOwner = false;
            synchronized (ACTIVE_RELAYS_LOCK) {
                if (closed.get()) {
                    return;
                }
                final int refs = refCount.decrementAndGet();
                if (refs <= 0 && closed.compareAndSet(false, true)) {
                    ACTIVE_RELAYS.remove(relayKey, this);
                    closeOwner = true;
                }
            }
            if (closeOwner) {
                owner.closePhysical();
            }
        }
    }

    private static final class TunnelConnection implements Runnable {
        private final ProxyConfig proxyConfig;
        private final BootstrapAddress target;
        private final Socket clientSocket;
        private final Set<Socket> activeSockets;

        TunnelConnection(final ProxyConfig proxyConfig,
                         final BootstrapAddress target,
                         final Socket clientSocket,
                         final Set<Socket> activeSockets) {
            this.proxyConfig = proxyConfig;
            this.target = target;
            this.clientSocket = clientSocket;
            this.activeSockets = activeSockets;
        }

        @Override
        public void run() {
            Socket proxySocket = null;
            try {
                configureDataSocket(clientSocket);
                proxySocket = openConnectedProxySocket(proxyConfig, target, false);
                configureDataSocket(proxySocket);
                activeSockets.add(proxySocket);

                try {
                    RELAY_EXECUTOR.execute(new Pipe(clientSocket.getInputStream(), proxySocket.getOutputStream(), clientSocket, proxySocket, activeSockets));
                    RELAY_EXECUTOR.execute(new Pipe(proxySocket.getInputStream(), clientSocket.getOutputStream(), proxySocket, clientSocket, activeSockets));
                } catch (RejectedExecutionException rejected) {
                    throw new IOException("CloudConnector relay thread pool saturated while scheduling TCP pipe task. maxRelayThreads="
                            + MAX_RELAY_THREADS, rejected);
                }
            } catch (Throwable t) {
                LOG.warn("[SDIA Kafka] SAP CC TCP relay connection failed for target "
                        + target.authority() + ": " + rootCauseMessage(t));
                closeSocket(clientSocket);
                closeSocket(proxySocket);
                activeSockets.remove(clientSocket);
                if (proxySocket != null) activeSockets.remove(proxySocket);
            }
        }
    }

    private static final class Pipe implements Runnable {
        private final InputStream in;
        private final OutputStream out;
        private final Socket closeA;
        private final Socket closeB;
        private final Set<Socket> activeSockets;

        Pipe(final InputStream in,
             final OutputStream out,
             final Socket closeA,
             final Socket closeB,
             final Set<Socket> activeSockets) {
            this.in = in;
            this.out = out;
            this.closeA = closeA;
            this.closeB = closeB;
            this.activeSockets = activeSockets;
        }

        @Override
        public void run() {
            final byte[] buffer = new byte[PIPE_BUFFER_SIZE];
            try {
                int read;
                while ((read = in.read(buffer)) >= 0) {
                    if (read > 0) {
                        out.write(buffer, 0, read);
                    }
                }
            } catch (Throwable ignored) {
            } finally {
                closeSocket(closeA);
                closeSocket(closeB);
                activeSockets.remove(closeA);
                activeSockets.remove(closeB);
            }
        }
    }

    private static void configureDataSocket(final Socket socket) {
        if (socket == null) return;
        try { socket.setTcpNoDelay(true); } catch (Throwable ignored) {}
        try { socket.setKeepAlive(true); } catch (Throwable ignored) {}
        try { socket.setReceiveBufferSize(65536); } catch (Throwable ignored) {}
        try { socket.setSendBufferSize(65536); } catch (Throwable ignored) {}
    }

    static List<BootstrapAddress> parseBootstrapAddresses(final String bootstrapServers) {
        final List<BootstrapAddress> result = new ArrayList<BootstrapAddress>(4);
        final String value = safeTrim(bootstrapServers);
        if (value == null) {
            return result;
        }

        int start = 0;
        final int len = value.length();
        while (start < len) {
            int comma = value.indexOf(',', start);
            if (comma == -1) comma = len;
            final String entry = safeTrim(value.substring(start, comma));
            if (entry != null) {
                result.add(parseBootstrapAddress(entry));
            }
            start = comma + 1;
        }
        return result;
    }

    private static BootstrapAddress parseBootstrapAddress(final String entry) {
        final String value = safeTrim(entry);
        if (value == null) {
            throw new IllegalArgumentException("Invalid empty Kafka bootstrap entry.");
        }
        final int colon = value.lastIndexOf(':');
        if (colon <= 0 || colon >= value.length() - 1) {
            throw new IllegalArgumentException("Invalid Kafka bootstrap entry. Expected host:port, got: " + value);
        }
        final String host = safeTrim(value.substring(0, colon));
        final String portStr = safeTrim(value.substring(colon + 1));
        if (host == null || portStr == null) {
            throw new IllegalArgumentException("Invalid Kafka bootstrap entry. Expected host:port, got: " + value);
        }
        try {
            final int port = Integer.parseInt(portStr);
            if (port <= 0 || port > 65535) {
                throw new NumberFormatException("port out of range");
            }
            return new BootstrapAddress(host, port);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid Kafka bootstrap port. Entry: " + value, e);
        }
    }

    static String safeTrim(final String value) {
        if (value == null) return null;
        final String v = value.trim();
        return v.length() == 0 ? null : v;
    }

    private static void closeSocket(final Socket socket) {
        if (socket != null) {
            try { socket.close(); } catch (Throwable ignored) {}
        }
    }

    static String toHex(final byte b) {
        final int v = b & 0xFF;
        final String h = Integer.toHexString(v).toUpperCase();
        return h.length() == 1 ? "0" + h : h;
    }

    private static String socks5RepMessage(final int rep) {
        switch (rep) {
            case 0x01: return "general SOCKS server failure";
            case 0x02: return "connection not allowed by ruleset";
            case 0x03: return "network unreachable";
            case 0x04: return "host unreachable";
            case 0x05: return "connection refused by destination host";
            case 0x06: return "TTL expired";
            case 0x07: return "command not supported or protocol error";
            case 0x08: return "address type not supported";
            default: return "unknown";
        }
    }

    static String rootCauseMessage(final Throwable t) {
        if (t == null) return "<unknown>";
        Throwable root = t;
        while (root.getCause() != null && root.getCause() != root) {
            root = root.getCause();
        }
        final String msg = root.getMessage();
        return msg == null || msg.trim().length() == 0 ? root.getClass().getName() : msg;
    }

    private static String displayOptionalLocationId(final String locationId) {
        final String value = safeTrim(locationId);
        if (value == null) {
            return "<not configured - optional/default Cloud Connector>";
        }
        if (value.length() <= 2) return "**";
        return value.charAt(0) + "***" + value.charAt(value.length() - 1);
    }

    public static final class TunnelConfig {
        final ProxyConfig proxyConfig;
        final List<BootstrapAddress> targets;
        final String localBindHost;

        public TunnelConfig(final ProxyConfig proxyConfig,
                            final List<BootstrapAddress> targets) {
            this(proxyConfig, targets, LOCAL_BIND_HOST);
        }

        public TunnelConfig(final ProxyConfig proxyConfig,
                            final List<BootstrapAddress> targets,
                            final String localBindHost) {
            this.proxyConfig = proxyConfig;
            this.targets = Collections.unmodifiableList(new ArrayList<BootstrapAddress>(targets));
            this.localBindHost = safeTrim(localBindHost) == null ? LOCAL_BIND_HOST : localBindHost.trim();
        }

        public ProxyConfig getProxyConfig() { return proxyConfig; }
        public List<BootstrapAddress> getTargets() { return targets; }
        public String getLocalBindHost() { return localBindHost; }
    }

    public static final class ProxyConfig {
        final String proxyHost;
        final int proxyPort;
        final String bearerToken;
        final String locationId;
        final Set<Integer> attemptedPorts;
        /**
         * Optional live token supplier. When set (via setLiveTokenSupplier after construction),
         * every SOCKS5 handshake calls it to get a fresh token instead of reusing the value
         * captured when this ProxyConfig was built. Plain field + setter (no new constructor
         * argument, no new public interface) to keep this change small and low-risk.
         *
         * Why this exists: a single relay (and its ProxyConfig) is shared by every CPI channel
         * bound to the same virtual host:port — only the first channel to start actually
         * resolves the token. Every individual Kafka TCP connection still performs its own
         * SOCKS5 handshake using this same ProxyConfig. If channels on the same port are
         * deployed minutes apart (normal when hand-editing several channels), the token captured
         * by the first channel can expire before a later channel's first connection, causing a
         * fast SOCKS5 JWT rejection that looks like Event.Smart.Kafka.Config.Broker.Metadata.Timeout
         * within milliseconds.
         */
        private volatile java.util.concurrent.Callable<String> liveTokenSupplier;

        public ProxyConfig(final String proxyHost,
                           final int proxyPort,
                           final String bearerToken,
                           final String locationId,
                           final Set<Integer> attemptedPorts) {
            final String host = safeTrim(proxyHost);
            if (host == null) {
                throw new IllegalArgumentException("proxyHost must not be empty.");
            }
            if (proxyPort <= 0 || proxyPort > 65535) {
                throw new IllegalArgumentException("Invalid proxyPort: " + proxyPort);
            }
            this.proxyHost = host;
            this.proxyPort = proxyPort;
            this.bearerToken = safeTrim(bearerToken);
            this.locationId = safeTrim(locationId);
            this.attemptedPorts = attemptedPorts == null
                    ? Collections.<Integer>emptySet()
                    : Collections.unmodifiableSet(new LinkedHashSet<Integer>(attemptedPorts));
        }

        /** Optional: supply a way to fetch a fresh token at handshake time. Safe no-op if never called. */
        public void setLiveTokenSupplier(final java.util.concurrent.Callable<String> supplier) {
            this.liveTokenSupplier = supplier;
        }

        boolean shouldOfferTokenAuth() {
            return hasBearerToken() || liveTokenSupplier != null;
        }

        /** Returns a fresh token if a live supplier was set and it succeeds; otherwise the original. */
        String resolveCurrentBearerToken() {
            final java.util.concurrent.Callable<String> supplier = this.liveTokenSupplier;
            if (supplier != null) {
                try {
                    final String fresh = supplier.call();
                    if (fresh != null && fresh.trim().length() > 0) {
                        return fresh.trim();
                    }
                } catch (Throwable ignored) {
                    // Fall through to the originally captured token.
                }
            }
            return bearerToken;
        }

        public String getProxyHost() { return proxyHost; }
        public int getProxyPort() { return proxyPort; }
        public String getBearerToken() { return bearerToken; }
        public String getLocationId() { return locationId; }
        public boolean hasBearerToken() { return bearerToken != null && bearerToken.length() > 0; }

        String maskedAuthority() {
            return proxyHost + ':' + proxyPort;
        }

        String authDescription() {
            return authDescription(shouldOfferTokenAuth());
        }

        String authDescription(final boolean offerTokenAuth) {
            return offerTokenAuth ? "SAP-JWT-0x80/offered + RFC1929 fallback" : "NO_AUTH only";
        }

        String offeredAuthMethodsDescription() {
            return offeredAuthMethodsDescription(shouldOfferTokenAuth());
        }

        String offeredAuthMethodsDescription(final boolean offerTokenAuth) {
            return offerTokenAuth ? "0x80,0x02,0x00" : "0x00";
        }
    }

    public static final class BootstrapAddress {
        public final String host;
        public final int port;

        public BootstrapAddress(final String host, final int port) {
            final String h = safeTrim(host);
            if (h == null) {
                throw new IllegalArgumentException("Bootstrap host must not be empty.");
            }
            if (port <= 0 || port > 65535) {
                throw new IllegalArgumentException("Invalid bootstrap port: " + port);
            }
            this.host = h;
            this.port = port;
        }

        public String authority() {
            return host + ':' + port;
        }

        @Override
        public String toString() {
            return authority();
        }
    }
}