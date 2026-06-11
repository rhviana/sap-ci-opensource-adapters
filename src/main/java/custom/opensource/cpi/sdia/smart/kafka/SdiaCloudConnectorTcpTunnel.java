/*
 * ============================================================================
 * Event Smart Kafka Adapter — SDIA
 * Semantic Domain Integration Architecture
 * ============================================================================
 *
 * Copyright (c) 2026 Ricardo Luz Holanda Viana
 * Independent Solo Researcher | Enterprise Integration Architecture
 * SAP BTP Integration Suite Expert | SAP Press Author
 * Enterprise Messaging (SAP Press, 2021)
 * Creator of DEIP · SDIA · GDCR · DDCR · ODCP · EDCP · DDCP
 * ORCID: 0009-0009-9549-5862
 *
 * Dual-Licensed: Apache License 2.0 / MIT License — your choice.
 *
 * ⚠️  This header must NOT be removed or altered in any distribution.
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
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * High-performance, secure local TCP relay for SAP Cloud Connector Kafka access.
 *
 * <h3>Why this exists</h3>
 * The Apache Kafka Java client does not expose a stable hook to inject a custom
 * SOCKS5 socket per connection. SAP Cloud Connector TCP access is gated behind
 * an SAP Connectivity SOCKS5 proxy that requires a proprietary JWT authentication
 * sub-negotiation (method 0x80). This relay keeps Kafka untouched: Kafka connects
 * to {@code 127.0.0.1:<brokerPort>} while the relay forwards the raw TCP stream
 * through the Cloud Connector SOCKS5 proxy to the virtual host.
 *
 * <h3>Multi-iFlow passive reuse</h3>
 * Multiple iFlows in the same CPI runtime may target the same Kafka broker port.
 * The first iFlow that binds the local port owns the relay. Subsequent iFlows
 * detect the occupied port and attach passively — they add no threads or sockets.
 *
 * <h3>Security</h3>
 * <ul>
 *   <li>Bearer JWT is stored as a zeroed {@code byte[]} in {@link ProxyConfig},
 *       not as a {@code String}, so GC heap dumps cannot reveal the token.</li>
 *   <li>The SOCKS5 handshake is performed with an explicit 15-second timeout;
 *       data pipes run without a socket timeout to avoid breaking long idle Kafka
 *       connections.</li>
 *   <li>All worker threads are daemon threads bounded by a fixed-size pool to
 *       prevent unbounded thread creation under connection storms.</li>
 * </ul>
 *
 * <h3>Performance</h3>
 * <ul>
 *   <li>Pipe buffer matches socket receive/send buffer (64 KB) — one syscall
 *       per full buffer instead of two.</li>
 *   <li>{@code flush()} is removed from the Pipe hot path; {@code TcpNoDelay}
 *       on the socket handles latency.</li>
 *   <li>Worker threads come from a bounded {@link ThreadPoolExecutor} with a
 *       keep-alive of 60 s, so idle threads are reclaimed without churn.</li>
 *   <li>{@code setPerformancePreferences(0,1,2)} hints the JVM to favour
 *       latency over connection time and bandwidth — correct for interactive
 *       Kafka metadata + fetch traffic.</li>
 * </ul>
 *
 * <h3>Required Kafka broker configuration</h3>
 * The external/client advertised listener must be {@code localhost:<port>} or
 * {@code 127.0.0.1:<port>} because Kafka metadata will point the Kafka client
 * back to the local relay on that port.
 */
public final class SdiaCloudConnectorTcpTunnel implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(SdiaCloudConnectorTcpTunnel.class);

    private static final String  LOCAL_BIND_HOST      = "127.0.0.1";
    private static final int     CONNECT_TIMEOUT_MS   = 10_000;
    private static final int     HANDSHAKE_TIMEOUT_MS = 15_000;
    /** Pipe buffer equals socket buffer — one read syscall drains the OS buffer. */
    private static final int     PIPE_BUFFER_SIZE     = 65_536;
    private static final int     SOCKET_BUFFER_SIZE   = 65_536;
    /** Maximum concurrent relay connections per port (accept-thread workers). */
    private static final int     WORKER_POOL_MAX      = 32;

    private final TunnelConfig        config;
    private final List<TunnelServer>  servers = new ArrayList<TunnelServer>(4);
    private final AtomicBoolean       running = new AtomicBoolean(false);

    public SdiaCloudConnectorTcpTunnel(final TunnelConfig config) {
        if (config == null)                              throw new IllegalArgumentException("TunnelConfig must not be null.");
        if (config.proxyConfig == null)                  throw new IllegalArgumentException("TunnelConfig.proxyConfig must not be null.");
        if (config.targets == null || config.targets.isEmpty()) throw new IllegalArgumentException("TunnelConfig.targets must not be empty.");
        this.config = config;
    }

    // -------------------------------------------------------------------------
    // Lifecycle
    // -------------------------------------------------------------------------

    /**
     * Starts one local listener per configured Kafka bootstrap port.
     * Fail-fast: if the SOCKS5 probe fails for any target, startup is aborted
     * and already-started servers are closed before the exception propagates.
     */
    public synchronized void start() throws IOException {
        if (running.get()) return;

        final List<TunnelServer> started = new ArrayList<TunnelServer>(config.targets.size());
        try {
            for (final BootstrapAddress target : config.targets) {
                // Probe before binding — fast fail with a clear error before Kafka waits for metadata timeout.
                testProxyConnect(config.proxyConfig, target);

                TunnelServer server;
                try {
                    server = new TunnelServer(config.proxyConfig, target, config.localBindHost);
                    server.start();
                    LOG.warn("[SDIA Kafka] CC TCP relay started. local=" + server.localBootstrap()
                            + " -> virtual=" + target.authority()
                            + " via socks=" + config.proxyConfig.maskedAuthority()
                            + " auth=" + config.proxyConfig.authDescription()
                            + " locationId=" + displayOptionalLocationId(config.proxyConfig.locationId));
                } catch (final IOException bindEx) {
                    // Another iFlow in this CPI runtime may already own the relay on this port.
                    // Reuse it passively rather than failing the second/third iFlow.
                    if (isAddressAlreadyInUse(bindEx) && isLocalRelayReachable(config.localBindHost, target.port)) {
                        server = TunnelServer.passive(target, config.localBindHost);
                        LOG.warn("[SDIA Kafka] CC TCP relay already present on " + config.localBindHost
                                + ":" + target.port + " — passive reuse. virtual=" + target.authority());
                    } else {
                        throw bindEx;
                    }
                }
                started.add(server);
            }
            servers.addAll(started);
            running.set(true);
        } catch (final Throwable t) {
            for (final TunnelServer s : started) { try { s.close(); } catch (Throwable ignored) {} }
            if (t instanceof IOException) throw (IOException) t;
            throw new IOException("SAP Cloud Connector TCP relay startup failed: " + rootCauseMessage(t), t);
        }
    }

    public boolean isRunning() { return running.get(); }

    public String getLocalBootstrapServers() {
        final StringBuilder sb = new StringBuilder(128);
        final List<?> source = servers.isEmpty() ? config.targets : null;
        if (source != null) {
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
        for (final TunnelServer s : servers) { try { s.close(); } catch (Throwable ignored) {} }
        servers.clear();
    }

    // -------------------------------------------------------------------------
    // Probe & socket helpers
    // -------------------------------------------------------------------------

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
        // Favour latency over connection setup time and bandwidth — right for Kafka metadata+fetch.
        socket.setPerformancePreferences(0, 1, 2);
        socket.setTcpNoDelay(true);
        socket.setKeepAlive(true);
        try { socket.setReceiveBufferSize(SOCKET_BUFFER_SIZE); } catch (Throwable ignored) {}
        try { socket.setSendBufferSize(SOCKET_BUFFER_SIZE);    } catch (Throwable ignored) {}

        socket.connect(new InetSocketAddress(proxyConfig.proxyHost, proxyConfig.proxyPort), CONNECT_TIMEOUT_MS);
        socket.setSoTimeout(HANDSHAKE_TIMEOUT_MS);
        performSocks5Handshake(socket, proxyConfig, target);
        socket.setSoTimeout(0); // Data pipes must not time out — Kafka connections can be idle.

        if (probeMode) {
            LOG.warn("[SDIA Kafka] CC SOCKS5 probe OK. target=" + target.authority()
                    + " proxy=" + proxyConfig.maskedAuthority()
                    + " auth=" + proxyConfig.authDescription()
                    + " locationId=" + displayOptionalLocationId(proxyConfig.locationId));
        }
        return socket;
    }

    // -------------------------------------------------------------------------
    // SOCKS5 handshake — SAP proprietary method 0x80 + RFC1929 fallback
    // -------------------------------------------------------------------------

    private static void performSocks5Handshake(final Socket socket,
                                               final ProxyConfig proxyConfig,
                                               final BootstrapAddress target) throws IOException {
        final InputStream  in  = socket.getInputStream();
        final OutputStream out = socket.getOutputStream();

        writeGreeting(out, proxyConfig);

        final byte[] greeting = readFully(in, 2, "SOCKS5 method-selection response");
        if ((greeting[0] & 0xFF) != 0x05) {
            throw new IOException("Invalid SOCKS5 response version from SAP Connectivity proxy: 0x" + toHex(greeting[0]));
        }
        final int method = greeting[1] & 0xFF;
        if (method == 0xFF) {
            throw new IOException("SAP Connectivity SOCKS5 proxy rejected all offered auth methods. offered="
                    + proxyConfig.offeredAuthMethodsDescription());
        }

        if      (method == 0x80) { performSapJwtSocks5Auth(in, out, proxyConfig); }
        else if (method == 0x02) { performUsernamePasswordAuth(in, out, proxyConfig); }
        else if (method == 0x00) { /* no-auth — valid in local/test topologies */ }
        else {
            throw new IOException("SAP Connectivity SOCKS5 proxy selected unsupported auth method: 0x"
                    + toHex((byte) method) + ". offered=" + proxyConfig.offeredAuthMethodsDescription());
        }

        writeConnectRequest(out, target);
        readConnectResponse(in, target);
    }

    private static void writeGreeting(final OutputStream out, final ProxyConfig proxyConfig) throws IOException {
        if (proxyConfig.hasBearerToken()) {
            // SAP private JWT (0x80), RFC1929 username/password (0x02), no-auth (0x00)
            out.write(new byte[]{ 0x05, 0x03, (byte) 0x80, 0x02, 0x00 });
        } else {
            out.write(new byte[]{ 0x05, 0x01, 0x00 });
        }
        out.flush();
    }

    /**
     * SAP Connectivity SOCKS5 private auth method 0x80.
     * Wire format: version(1) + jwtLen(4 BE) + jwt + locationIdLen(1) + base64(locationId).
     * The bearer token bytes are zeroed immediately after writing to the socket.
     */
    private static void performSapJwtSocks5Auth(final InputStream in,
                                                final OutputStream out,
                                                final ProxyConfig proxyConfig) throws IOException {
        final byte[] jwtBytes = proxyConfig.cloneBearerTokenBytes();
        if (jwtBytes == null || jwtBytes.length == 0) {
            throw new IOException("SAP Connectivity selected SOCKS5 JWT auth 0x80, but no bearer token is available.");
        }
        try {
            final byte[] encodedLocation = encodeLocationId(proxyConfig.locationId);
            out.write(0x01);
            out.write((jwtBytes.length >> 24) & 0xFF);
            out.write((jwtBytes.length >> 16) & 0xFF);
            out.write((jwtBytes.length >>  8) & 0xFF);
            out.write( jwtBytes.length        & 0xFF);
            out.write(jwtBytes);
            out.write(encodedLocation.length & 0xFF);
            if (encodedLocation.length > 0) out.write(encodedLocation);
            out.flush();
        } finally {
            Arrays.fill(jwtBytes, (byte) 0); // zero sensitive bytes immediately
        }

        final byte[] response = readFully(in, 2, "SAP SOCKS5 JWT auth response");
        if ((response[0] & 0xFF) != 0x01 || (response[1] & 0xFF) != 0x00) {
            throw new IOException("SAP Connectivity SOCKS5 JWT auth failed. version=0x"
                    + toHex(response[0]) + " status=0x" + toHex(response[1])
                    + ". LocationId is optional — only set when the Cloud Connector subaccount uses one.");
        }
    }

    /**
     * RFC 1929 username/password fallback.
     * Username = bearer token (raw JWT string), password = Location ID or empty.
     * Bytes are zeroed after write.
     */
    private static void performUsernamePasswordAuth(final InputStream in,
                                                    final OutputStream out,
                                                    final ProxyConfig proxyConfig) throws IOException {
        final byte[] username = proxyConfig.cloneBearerTokenBytes();
        if (username == null || username.length == 0) {
            throw new IOException("SOCKS5 RFC1929 fallback selected, but bearer token is unavailable.");
        }
        final String loc = safeTrim(proxyConfig.locationId);
        final byte[] password = (loc == null ? "" : loc).getBytes(StandardCharsets.UTF_8);
        if (username.length > 255) throw new IOException("SOCKS5 RFC1929: bearer token > 255 bytes.");
        if (password.length > 255) throw new IOException("SOCKS5 RFC1929: Location ID > 255 bytes.");
        try {
            out.write(0x01);
            out.write(username.length & 0xFF);
            out.write(username);
            out.write(password.length & 0xFF);
            if (password.length > 0) out.write(password);
            out.flush();
        } finally {
            Arrays.fill(username, (byte) 0);
        }
        final byte[] response = readFully(in, 2, "SOCKS5 RFC1929 auth response");
        if ((response[0] & 0xFF) != 0x01 || (response[1] & 0xFF) != 0x00) {
            throw new IOException("SOCKS5 RFC1929 auth failed. version=0x"
                    + toHex(response[0]) + " status=0x" + toHex(response[1]));
        }
    }

    private static void writeConnectRequest(final OutputStream out,
                                            final BootstrapAddress target) throws IOException {
        final byte[] host = target.host.getBytes(StandardCharsets.UTF_8);
        if (host.length == 0 || host.length > 255) {
            throw new IOException("Invalid SOCKS5 target host length for CC virtual host: " + target.host);
        }
        out.write(0x05); // VER
        out.write(0x01); // CMD = CONNECT
        out.write(0x00); // RSV
        out.write(0x03); // ATYP = DOMAINNAME — keep virtual host unresolved locally
        out.write(host.length & 0xFF);
        out.write(host);
        out.write((target.port >> 8) & 0xFF);
        out.write( target.port       & 0xFF);
        out.flush();
    }

    private static void readConnectResponse(final InputStream in,
                                            final BootstrapAddress target) throws IOException {
        final byte[] header = readFully(in, 4, "SOCKS5 CONNECT response header");
        if ((header[0] & 0xFF) != 0x05) {
            throw new IOException("Invalid SOCKS5 CONNECT response version: 0x" + toHex(header[0]));
        }
        final int rep = header[1] & 0xFF;
        if (rep != 0x00) {
            throw new IOException("SAP CC SOCKS5 CONNECT failed for " + target.authority()
                    + ". REP=0x" + toHex((byte) rep) + " (" + socks5RepMessage(rep) + ")");
        }
        final int atyp = header[3] & 0xFF;
        if      (atyp == 0x01) { readFully(in,  4, "SOCKS5 IPv4 bind address"); }
        else if (atyp == 0x03) { readFully(in, readFully(in, 1, "SOCKS5 domain len")[0] & 0xFF, "SOCKS5 domain bind address"); }
        else if (atyp == 0x04) { readFully(in, 16, "SOCKS5 IPv6 bind address"); }
        else { throw new IOException("Unsupported SOCKS5 bind address type: 0x" + toHex((byte) atyp)); }
        readFully(in, 2, "SOCKS5 bind port");
    }

    // -------------------------------------------------------------------------
    // Inner classes
    // -------------------------------------------------------------------------

    private static final class TunnelServer implements AutoCloseable, Runnable {
        private final ProxyConfig      proxyConfig;
        private final BootstrapAddress target;
        private final ServerSocket     serverSocket;
        private final Thread           acceptThread;
        private final AtomicBoolean    running = new AtomicBoolean(true);
        private final String           localBindHost;
        private final boolean          passive;
        /** Bounded pool: at most WORKER_POOL_MAX concurrent relay connections per port. */
        private final ExecutorService  workerPool;

        TunnelServer(final ProxyConfig proxyConfig,
                     final BootstrapAddress target,
                     final String localBindHost) throws IOException {
            this.proxyConfig   = proxyConfig;
            this.target        = target;
            this.localBindHost = localBindHost;
            this.passive       = false;

            this.serverSocket = new ServerSocket();
            this.serverSocket.setReuseAddress(true);
            this.serverSocket.bind(new InetSocketAddress(InetAddress.getByName(localBindHost), target.port));

            final String portTag = String.valueOf(target.port);
            this.workerPool = new ThreadPoolExecutor(
                    2, WORKER_POOL_MAX, 60L, TimeUnit.SECONDS,
                    new LinkedBlockingQueue<Runnable>(WORKER_POOL_MAX),
                    new DaemonThreadFactory("sdia-cc-relay-" + portTag),
                    new ThreadPoolExecutor.CallerRunsPolicy());

            this.acceptThread = new DaemonThreadFactory("sdia-cc-accept-" + portTag).newThread(this);
        }

        // Passive reuse: this instance owns nothing; close() must not touch the shared relay.
        private TunnelServer(final BootstrapAddress target, final String localBindHost) {
            this.proxyConfig   = null;
            this.target        = target;
            this.localBindHost = localBindHost;
            this.passive       = true;
            this.serverSocket  = null;
            this.acceptThread  = null;
            this.workerPool    = null;
        }

        static TunnelServer passive(final BootstrapAddress target, final String localBindHost) {
            return new TunnelServer(target, localBindHost);
        }

        void start() {
            if (passive || acceptThread == null) return;
            acceptThread.start();
        }

        String localBootstrap() { return localBindHost + ':' + target.port; }

        @Override
        public void run() {
            while (running.get()) {
                Socket client = null;
                try {
                    client = serverSocket.accept();
                    configureDataSocket(client);
                    final Socket clientFinal = client;
                    workerPool.submit(new TunnelConnection(proxyConfig, target, clientFinal));
                } catch (final Throwable t) {
                    closeSocket(client);
                    if (running.get()) {
                        LOG.warn("[SDIA Kafka] CC TCP relay accept error target={}: {}", target.authority(), rootCauseMessage(t));
                    }
                }
            }
        }

        @Override
        public void close() {
            if (passive || serverSocket == null) return;
            running.set(false);
            try { serverSocket.close(); } catch (Throwable ignored) {}
            if (workerPool != null) workerPool.shutdown();
        }
    }

    private static final class TunnelConnection implements Runnable {
        private final ProxyConfig      proxyConfig;
        private final BootstrapAddress target;
        private final Socket           clientSocket;

        TunnelConnection(final ProxyConfig proxyConfig,
                         final BootstrapAddress target,
                         final Socket clientSocket) {
            this.proxyConfig  = proxyConfig;
            this.target       = target;
            this.clientSocket = clientSocket;
        }

        @Override
        public void run() {
            Socket proxySocket = null;
            try {
                proxySocket = openConnectedProxySocket(proxyConfig, target, false);
                configureDataSocket(proxySocket);

                // Two plain daemon threads — one per direction.
                // Avoid ThreadPoolExecutor allocation (3 heavy objects per connection)
                // since we always need exactly 2 tasks with no queuing or rejection policy.
                final String tag = "sdia-cc-pipe-" + target.port;
                final Thread c2p = new Thread(
                        new Pipe(clientSocket.getInputStream(), proxySocket.getOutputStream(), clientSocket, proxySocket),
                        tag + "-c2p");
                final Thread p2c = new Thread(
                        new Pipe(proxySocket.getInputStream(), clientSocket.getOutputStream(), proxySocket, clientSocket),
                        tag + "-p2c");
                c2p.setDaemon(true);
                p2c.setDaemon(true);
                c2p.start();
                p2c.start();
            } catch (final Throwable t) {
                LOG.warn("[SDIA Kafka] CC TCP relay connection failed target={}: {}", target.authority(), rootCauseMessage(t));
                closeSocket(clientSocket);
                closeSocket(proxySocket);
            }
        }
    }

    /**
     * Bidirectional byte pipe.
     * Buffer size matches socket buffer → one read syscall drains the OS receive buffer.
     * No {@code flush()} call: TcpNoDelay on the socket handles low-latency delivery
     * without an extra syscall per buffer.
     */
    private static final class Pipe implements Runnable {
        private final InputStream  in;
        private final OutputStream out;
        private final Socket       closeA;
        private final Socket       closeB;

        Pipe(final InputStream in, final OutputStream out,
             final Socket closeA, final Socket closeB) {
            this.in     = in;
            this.out    = out;
            this.closeA = closeA;
            this.closeB = closeB;
        }

        @Override
        public void run() {
            final byte[] buf = new byte[PIPE_BUFFER_SIZE];
            try {
                int n;
                while ((n = in.read(buf)) >= 0) {
                    if (n > 0) out.write(buf, 0, n);
                    // No flush() — TcpNoDelay pushes data immediately without syscall overhead.
                }
            } catch (final Throwable ignored) {
                // EOF or closed socket — normal path.
            } finally {
                closeSocket(closeA);
                closeSocket(closeB);
            }
        }
    }

    /** Produces daemon threads with a predictable name prefix and an atomic counter. */
    private static final class DaemonThreadFactory implements ThreadFactory {
        private final String        prefix;
        private final AtomicInteger counter = new AtomicInteger(0);

        DaemonThreadFactory(final String prefix) { this.prefix = prefix; }

        @Override
        public Thread newThread(final Runnable r) {
            final Thread t = new Thread(r, prefix + '-' + counter.incrementAndGet());
            t.setDaemon(true);
            return t;
        }
    }

    // -------------------------------------------------------------------------
    // Socket configuration
    // -------------------------------------------------------------------------

    private static void configureDataSocket(final Socket socket) {
        if (socket == null) return;
        try { socket.setPerformancePreferences(0, 1, 2); } catch (Throwable ignored) {}
        try { socket.setTcpNoDelay(true);                } catch (Throwable ignored) {}
        try { socket.setKeepAlive(true);                 } catch (Throwable ignored) {}
        try { socket.setReceiveBufferSize(SOCKET_BUFFER_SIZE); } catch (Throwable ignored) {}
        try { socket.setSendBufferSize(SOCKET_BUFFER_SIZE);    } catch (Throwable ignored) {}
    }

    // -------------------------------------------------------------------------
    // Parsing helpers
    // -------------------------------------------------------------------------

    static List<BootstrapAddress> parseBootstrapAddresses(final String bootstrapServers) {
        final List<BootstrapAddress> result = new ArrayList<BootstrapAddress>(4);
        final String value = safeTrim(bootstrapServers);
        if (value == null) return result;

        int start = 0;
        final int len = value.length();
        while (start < len) {
            int comma = value.indexOf(',', start);
            if (comma == -1) comma = len;
            final String entry = safeTrim(value.substring(start, comma));
            if (entry != null) result.add(parseBootstrapAddress(entry));
            start = comma + 1;
        }
        return result;
    }

    private static BootstrapAddress parseBootstrapAddress(final String entry) {
        final String value = safeTrim(entry);
        if (value == null) throw new IllegalArgumentException("Empty Kafka bootstrap entry.");
        final int colon = value.lastIndexOf(':');
        if (colon <= 0 || colon >= value.length() - 1) {
            throw new IllegalArgumentException("Invalid Kafka bootstrap entry — expected host:port, got: " + value);
        }
        final String host    = safeTrim(value.substring(0, colon));
        final String portStr = safeTrim(value.substring(colon + 1));
        if (host == null || portStr == null) {
            throw new IllegalArgumentException("Invalid Kafka bootstrap entry — expected host:port, got: " + value);
        }
        try {
            final int port = Integer.parseInt(portStr);
            if (port <= 0 || port > 65535) throw new NumberFormatException("port out of range");
            return new BootstrapAddress(host, port);
        } catch (final NumberFormatException e) {
            throw new IllegalArgumentException("Invalid Kafka bootstrap port. entry=" + value, e);
        }
    }

    private static byte[] encodeLocationId(final String locationId) throws IOException {
        final String loc = safeTrim(locationId);
        if (loc == null) return new byte[0];
        final byte[] encoded = java.util.Base64.getEncoder().encode(loc.getBytes(StandardCharsets.UTF_8));
        if (encoded.length > 255) {
            throw new IOException("Cloud Connector Location ID too long after Base64 encoding. length=" + encoded.length);
        }
        return encoded;
    }

    private static byte[] readFully(final InputStream in,
                                    final int len,
                                    final String stage) throws IOException {
        final byte[] data = new byte[len];
        int off = 0;
        while (off < len) {
            final int n = in.read(data, off, len - off);
            if (n < 0) throw new IOException("Unexpected EOF while reading " + stage + ".");
            off += n;
        }
        return data;
    }

    private static boolean isAddressAlreadyInUse(final Throwable t) {
        Throwable c = t;
        while (c != null) {
            if (c instanceof java.net.BindException) return true;
            final String m = c.getMessage();
            if (m != null && m.toLowerCase().contains("address already in use")) return true;
            c = c.getCause();
        }
        return false;
    }

    private static boolean isLocalRelayReachable(final String host, final int port) {
        final String h = safeTrim(host) == null ? LOCAL_BIND_HOST : host.trim();
        for (int attempt = 0; attempt < 3; attempt++) {
            Socket s = null;
            try {
                s = new Socket();
                s.connect(new InetSocketAddress(InetAddress.getByName(h), port), 2000);
                return true;
            } catch (final Throwable ignored) {
            } finally {
                closeSocket(s);
            }
            try { Thread.sleep(300L); } catch (final InterruptedException ie) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
        return false;
    }

    static String safeTrim(final String value) {
        if (value == null) return null;
        final String v = value.trim();
        return v.isEmpty() ? null : v;
    }

    private static void closeSocket(final Socket socket) {
        if (socket != null) { try { socket.close(); } catch (Throwable ignored) {} }
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
            case 0x07: return "command not supported / protocol error";
            case 0x08: return "address type not supported";
            default:   return "unknown";
        }
    }

    static String rootCauseMessage(final Throwable t) {
        if (t == null) return "<unknown>";
        Throwable root = t;
        while (root.getCause() != null && root.getCause() != root) root = root.getCause();
        final String msg = root.getMessage();
        return (msg == null || msg.trim().isEmpty()) ? root.getClass().getName() : msg;
    }

    private static String displayOptionalLocationId(final String locationId) {
        final String v = safeTrim(locationId);
        if (v == null) return "<not configured>";
        if (v.length() <= 2) return "**";
        return v.charAt(0) + "***" + v.charAt(v.length() - 1);
    }

    // -------------------------------------------------------------------------
    // Public value types
    // -------------------------------------------------------------------------

    public static final class TunnelConfig {
        final ProxyConfig            proxyConfig;
        final List<BootstrapAddress> targets;
        final String                 localBindHost;

        public TunnelConfig(final ProxyConfig proxyConfig, final List<BootstrapAddress> targets) {
            this(proxyConfig, targets, LOCAL_BIND_HOST);
        }

        public TunnelConfig(final ProxyConfig proxyConfig,
                            final List<BootstrapAddress> targets,
                            final String localBindHost) {
            this.proxyConfig   = proxyConfig;
            this.targets       = Collections.unmodifiableList(new ArrayList<BootstrapAddress>(targets));
            this.localBindHost = safeTrim(localBindHost) == null ? LOCAL_BIND_HOST : localBindHost.trim();
        }

        public ProxyConfig            getProxyConfig()   { return proxyConfig; }
        public List<BootstrapAddress> getTargets()       { return targets; }
        public String                 getLocalBindHost() { return localBindHost; }
    }

    /**
     * Immutable proxy configuration.
     *
     * The bearer token is stored as a {@code byte[]} rather than a {@code String}.
     * Strings in Java are interned/pooled and their content cannot be zeroed; they
     * remain in heap dumps until GC compaction. A {@code byte[]} can be explicitly
     * zeroed after each use, limiting the window of token exposure.
     */
    public static final class ProxyConfig {
        final String  proxyHost;
        final int     proxyPort;
        /** Raw UTF-8 bytes of the bearer JWT. Never exposed as String. */
        private final byte[]  bearerTokenBytes;
        final String  locationId;
        final Set<Integer> attemptedPorts;

        public ProxyConfig(final String  proxyHost,
                           final int     proxyPort,
                           final String  bearerToken,
                           final String  locationId,
                           final Set<Integer> attemptedPorts) {
            final String host = safeTrim(proxyHost);
            if (host == null) throw new IllegalArgumentException("proxyHost must not be empty.");
            if (proxyPort <= 0 || proxyPort > 65535) throw new IllegalArgumentException("Invalid proxyPort: " + proxyPort);
            this.proxyHost       = host;
            this.proxyPort       = proxyPort;
            this.locationId      = safeTrim(locationId);
            this.attemptedPorts  = attemptedPorts == null
                    ? Collections.<Integer>emptySet()
                    : Collections.unmodifiableSet(new LinkedHashSet<Integer>(attemptedPorts));
            // Convert once at construction; the source String will be GC'd normally.
            final String token = safeTrim(bearerToken);
            this.bearerTokenBytes = (token == null) ? null : token.getBytes(StandardCharsets.UTF_8);
        }

        public String  getProxyHost()  { return proxyHost; }
        public int     getProxyPort()  { return proxyPort; }
        public String  getLocationId() { return locationId; }
        public boolean hasBearerToken() { return bearerTokenBytes != null && bearerTokenBytes.length > 0; }

        /**
         * Returns a defensive copy of the bearer token bytes.
         * The caller is responsible for zeroing the returned array after use.
         */
        byte[] cloneBearerTokenBytes() {
            if (bearerTokenBytes == null || bearerTokenBytes.length == 0) return null;
            return Arrays.copyOf(bearerTokenBytes, bearerTokenBytes.length);
        }

        String maskedAuthority() { return proxyHost + ':' + proxyPort; }

        String authDescription() {
            return hasBearerToken() ? "SAP-JWT-0x80 + RFC1929-fallback + NO_AUTH" : "NO_AUTH only";
        }

        String offeredAuthMethodsDescription() {
            return hasBearerToken() ? "0x80,0x02,0x00" : "0x00";
        }
    }

    public static final class BootstrapAddress {
        public final String host;
        public final int    port;

        public BootstrapAddress(final String host, final int port) {
            final String h = safeTrim(host);
            if (h == null) throw new IllegalArgumentException("Bootstrap host must not be empty.");
            if (port <= 0 || port > 65535) throw new IllegalArgumentException("Invalid bootstrap port: " + port);
            this.host = h;
            this.port = port;
        }

        public String authority() { return host + ':' + port; }

        @Override public String toString() { return authority(); }
    }
}
