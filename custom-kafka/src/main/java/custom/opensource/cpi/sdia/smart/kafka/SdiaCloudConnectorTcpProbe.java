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

import com.sap.it.api.ITApiFactory;
import com.sap.it.api.ccs.adapter.CloudConnectorContext;
import com.sap.it.api.ccs.adapter.CloudConnectorProperties;
import com.sap.it.api.ccs.adapter.ConnectionType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Fail-fast Cloud Connector TCP probe for Kafka On-Premise mode.
 *
 * This class intentionally does NOT use KafkaConsumer.
 * It proves the raw TCP path first:
 * CPI runtime -> SAP Connectivity SOCKS5 proxy -> Cloud Connector virtual host -> Kafka broker port.
 *
 * If this probe fails, changing Kafka poll/offset/seek logic is pointless.
 */
public final class SdiaCloudConnectorTcpProbe {

    private static final Logger LOG = LoggerFactory.getLogger(SdiaCloudConnectorTcpProbe.class);

    private static final String LOCATION_HEADER = "SAP-Connectivity-SCC-Location_ID";
    private static final int DEFAULT_SOCKS5_PORT = 20004;

    private SdiaCloudConnectorTcpProbe() {}

    /**
     * Resolve Cloud Connector proxy information and probe every virtual Kafka bootstrap address.
     */
    public static SdiaCloudConnectorTcpTunnel.TunnelConfig resolveAndProbe(final String virtualBootstrapServers,
                                                                           final String locationId) throws Exception {
        final List<SdiaCloudConnectorTcpTunnel.BootstrapAddress> targets =
                SdiaCloudConnectorTcpTunnel.parseBootstrapAddresses(virtualBootstrapServers);
        if (targets.isEmpty()) {
            throw new IllegalArgumentException("No valid virtual Kafka bootstrap host:port found for On-Premise mode: "
                    + virtualBootstrapServers);
        }

        final CloudConnectorProperties ccProps = loadCloudConnectorProperties();
        final Map<String, String> headers = loadAdditionalHeaders(ccProps, locationId);
        final String token = resolveBearerToken(headers);
        final String proxyHost = resolveProxyHost(ccProps);
        final List<Integer> candidatePorts = resolveSocks5CandidatePorts(ccProps);

        if (SdiaCloudConnectorTcpTunnel.safeTrim(proxyHost) == null) {
            throw new IllegalStateException("SAP Connectivity proxy host is empty. CloudConnectorProperties.getProxyHost() and environment variable onpremise_proxy_host are unavailable.");
        }
        if (candidatePorts.isEmpty()) {
            throw new IllegalStateException("SAP Connectivity SOCKS5 proxy port is empty. Expected onpremise_socks5_proxy_port or standard port 20004.");
        }

        LOG.warn("[SDIA Kafka] SAP CC TCP probe resolving. virtualBootstrap=" + virtualBootstrapServers
                + " | proxyHost=" + proxyHost
                + " | candidatePorts=" + candidatePorts
                + " | tokenAvailable=" + (token != null)
                + " | locationId=" + displayOptionalLocationId(locationId)
                + " | headerKeys=" + maskHeaderKeys(headers));

        IOException lastError = null;
        final Set<Integer> attempted = new LinkedHashSet<Integer>();
        for (Integer candidatePort : candidatePorts) {
            if (candidatePort == null || candidatePort.intValue() <= 0) {
                continue;
            }
            attempted.add(candidatePort);
            final SdiaCloudConnectorTcpTunnel.ProxyConfig proxyConfig =
                    new SdiaCloudConnectorTcpTunnel.ProxyConfig(proxyHost, candidatePort.intValue(), token, locationId, attempted);
            // Re-resolve the token on every individual SOCKS5 handshake rather than reusing the
            // value captured above. A single relay (and this ProxyConfig) is shared by every CPI
            // channel bound to the same virtual host:port, and channels are sometimes deployed
            // minutes apart, so the token captured here can expire before a later channel's
            // KafkaConsumer opens its own connection. Plain setter, no constructor/signature change.
            final CloudConnectorProperties capturedCcProps = ccProps;
            final String capturedLocationId = locationId;
            proxyConfig.setLiveTokenSupplier(new java.util.concurrent.Callable<String>() {
                @Override
                public String call() {
                    try {
                        return resolveBearerToken(loadAdditionalHeaders(capturedCcProps, capturedLocationId));
                    } catch (Throwable t) {
                        return null;
                    }
                }
            });
            try {
                probeAllTargets(proxyConfig, targets);
                LOG.warn("[SDIA Kafka] SAP CC TCP probe selected SOCKS5 proxy "
                        + proxyHost + ':' + candidatePort
                        + " | virtualBootstrap=" + virtualBootstrapServers
                        + " | locationId=" + displayOptionalLocationId(locationId));
                return new SdiaCloudConnectorTcpTunnel.TunnelConfig(proxyConfig, targets);
            } catch (IOException probeEx) {
                lastError = probeEx;
                LOG.warn("[SDIA Kafka] SAP CC TCP probe failed on SOCKS5 candidate "
                        + proxyHost + ':' + candidatePort + " | detail="
                        + SdiaCloudConnectorTcpTunnel.rootCauseMessage(probeEx));
            }
        }

        throw new IOException("SAP Cloud Connector TCP probe failed for all SOCKS5 candidates. virtualBootstrap="
                + virtualBootstrapServers
                + " | proxyHost=" + proxyHost
                + " | attemptedPorts=" + attempted
                + " | lastError=" + SdiaCloudConnectorTcpTunnel.rootCauseMessage(lastError), lastError);
    }

    private static void probeAllTargets(final SdiaCloudConnectorTcpTunnel.ProxyConfig proxyConfig,
                                        final List<SdiaCloudConnectorTcpTunnel.BootstrapAddress> targets) throws IOException {
        for (SdiaCloudConnectorTcpTunnel.BootstrapAddress target : targets) {
            SdiaCloudConnectorTcpTunnel.testProxyConnect(proxyConfig, target);
        }
    }

    private static CloudConnectorProperties loadCloudConnectorProperties() {
        try {
            final CloudConnectorContext context = new CloudConnectorContext();
            // ADK exposes Cloud Connector properties through HTTP context. TCP is handled by SOCKS5 below.
            context.setConnectionType(ConnectionType.HTTP);
            final CloudConnectorProperties props = ITApiFactory.getService(CloudConnectorProperties.class, context);
            if (props == null) {
                throw new IllegalStateException("ITApiFactory returned null CloudConnectorProperties service.");
            }
            return props;
        } catch (Throwable t) {
            throw new IllegalStateException("SAP CloudConnectorProperties service is not available in this CPI runtime: "
                    + SdiaCloudConnectorTcpTunnel.rootCauseMessage(t), t);
        }
    }

    private static Map<String, String> loadAdditionalHeaders(final CloudConnectorProperties ccProps,
                                                             final String locationId) {
        final Map<String, String> result = new HashMap<String, String>();
        try {
            final Map<String, String> headers = ccProps.getAdditionalHeaders();
            if (headers != null) {
                result.putAll(headers);
            }
        } catch (Throwable t) {
            LOG.warn("[SDIA Kafka] CloudConnectorProperties.getAdditionalHeaders() failed. Continuing with environment/token fallback. Detail="
                    + SdiaCloudConnectorTcpTunnel.rootCauseMessage(t));
        }

        final String loc = SdiaCloudConnectorTcpTunnel.safeTrim(locationId);
        if (loc != null && !containsHeaderIgnoreCase(result, LOCATION_HEADER)) {
            result.put(LOCATION_HEADER, loc);
        }
        return result;
    }

    private static String resolveProxyHost(final CloudConnectorProperties ccProps) {
        String value = firstNonEmpty(
                getEnvOrProperty("onpremise_proxy_host"),
                getEnvOrProperty("ONPREMISE_PROXY_HOST"),
                getEnvOrProperty("connectivity_proxy_host"),
                getEnvOrProperty("CONNECTIVITY_PROXY_HOST"));
        if (value != null) {
            return value;
        }
        try {
            return SdiaCloudConnectorTcpTunnel.safeTrim(ccProps.getProxyHost());
        } catch (Throwable t) {
            throw new IllegalStateException("CloudConnectorProperties.getProxyHost() failed: "
                    + SdiaCloudConnectorTcpTunnel.rootCauseMessage(t), t);
        }
    }

    private static List<Integer> resolveSocks5CandidatePorts(final CloudConnectorProperties ccProps) {
        final LinkedHashSet<Integer> ports = new LinkedHashSet<Integer>(4);

        addPort(ports, getEnvOrProperty("onpremise_socks5_proxy_port"));
        addPort(ports, getEnvOrProperty("ONPREMISE_SOCKS5_PROXY_PORT"));
        addPort(ports, getEnvOrProperty("connectivity_socks5_proxy_port"));
        addPort(ports, getEnvOrProperty("CONNECTIVITY_SOCKS5_PROXY_PORT"));
        addPort(ports, getEnvOrProperty("socks5_proxy_port"));
        addPort(ports, getEnvOrProperty("SOCKS5_PROXY_PORT"));

        // In many SAP BTP runtimes TCP/SOCKS5 is exposed on 20004.
        addPort(ports, String.valueOf(DEFAULT_SOCKS5_PORT));

        // Last fallback: the ADK CloudConnectorProperties proxy port. In HTTP mode this may be 20003
        // and reject CONNECT/HTTP as 405, but probing it costs milliseconds and makes diagnostics explicit.
        try {
            addPort(ports, String.valueOf(ccProps.getProxyPort()));
        } catch (Throwable ignored) {}

        return new ArrayList<Integer>(ports);
    }

    private static String resolveBearerToken(final Map<String, String> headers) {
        String token = extractBearerToken(headers);
        if (token != null) {
            return token;
        }
        token = firstNonEmpty(
                getEnvOrProperty("CONNECTIVITY_AUTHORIZATION_TOKEN"),
                getEnvOrProperty("SAP_CONNECTIVITY_AUTHENTICATION_TOKEN"),
                getEnvOrProperty("SAP_CONNECTIVITY_TOKEN"),
                getEnvOrProperty("ONPREMISE_PROXY_TOKEN"));
        if (token == null) {
            return null;
        }
        final String lower = token.toLowerCase();
        if (lower.startsWith("bearer ")) {
            return SdiaCloudConnectorTcpTunnel.safeTrim(token.substring(7));
        }
        return SdiaCloudConnectorTcpTunnel.safeTrim(token);
    }

    private static String extractBearerToken(final Map<String, String> headers) {
        if (headers == null || headers.isEmpty()) {
            return null;
        }
        for (Map.Entry<String, String> e : headers.entrySet()) {
            final String key = e.getKey();
            final String value = SdiaCloudConnectorTcpTunnel.safeTrim(e.getValue());
            if (key == null || value == null) {
                continue;
            }
            final String k = key.toLowerCase();
            if (k.indexOf("authorization") < 0 && k.indexOf("authentication") < 0) {
                continue;
            }
            final String lower = value.toLowerCase();
            if (lower.startsWith("bearer ")) {
                return SdiaCloudConnectorTcpTunnel.safeTrim(value.substring(7));
            }
        }
        return null;
    }

    private static boolean containsHeaderIgnoreCase(final Map<String, String> headers, final String key) {
        if (headers == null || key == null) return false;
        for (String k : headers.keySet()) {
            if (key.equalsIgnoreCase(k)) return true;
        }
        return false;
    }

    private static String getEnvOrProperty(final String key) {
        if (key == null) return null;
        String value = null;
        try { value = System.getProperty(key); } catch (Throwable ignored) {}
        value = SdiaCloudConnectorTcpTunnel.safeTrim(value);
        if (value != null) return value;
        try { value = System.getenv(key); } catch (Throwable ignored) {}
        return SdiaCloudConnectorTcpTunnel.safeTrim(value);
    }

    private static String firstNonEmpty(final String... values) {
        if (values == null) return null;
        for (String v : values) {
            final String t = SdiaCloudConnectorTcpTunnel.safeTrim(v);
            if (t != null) return t;
        }
        return null;
    }

    private static void addPort(final Set<Integer> ports, final String value) {
        final String v = SdiaCloudConnectorTcpTunnel.safeTrim(value);
        if (v == null) return;
        try {
            final int p = Integer.parseInt(v);
            if (p > 0 && p <= 65535) {
                ports.add(Integer.valueOf(p));
            }
        } catch (Throwable ignored) {}
    }

    private static String maskHeaderKeys(final Map<String, String> headers) {
        if (headers == null || headers.isEmpty()) {
            return "<none>";
        }
        return headers.keySet().toString();
    }

    private static String displayOptionalLocationId(final String locationId) {
        final String value = SdiaCloudConnectorTcpTunnel.safeTrim(locationId);
        if (value == null) {
            return "<not configured - optional/default Cloud Connector>";
        }
        if (value.length() <= 2) return "**";
        return value.charAt(0) + "***" + value.charAt(value.length() - 1);
    }
}
