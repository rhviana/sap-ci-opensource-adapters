/*
 * ============================================================================
 * Event Smart Kafka Adapter
 * SDIA — Semantic Domain Integration Architecture
 * ============================================================================
 *
 * Copyright (c) 2026 Ricardo Luz Holanda Viana
 * Independent Solo Researcher | Enterprise Integration Architecture
 * SAP BTP Integration Suite Expert | Developer | SAP Press Author
 * Enterprise Messaging (SAP Press, 2021)
 * Creator of DEIP · SDIA · GDCR · DDCR · ODCP · EDCP · DDCP
 * ORCID: 0009-0009-9549-5862
 *
 * ----------------------------------------------------------------------------
 * Dual-Licensed under:
 *
 *   Apache License, Version 2.0
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 *   MIT License
 *   https://opensource.org/licenses/MIT
 *
 * You may use this software under either license at your option.
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under these licenses is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * ----------------------------------------------------------------------------
 *
 * ⚠️  NOTICE — This header must NOT be removed or altered in any distribution,
 *     derivative work, or reuse of this source code, in whole or in part.
 *     Removal of this header constitutes a violation of the license terms
 *     and applicable intellectual property rights.
 *
 * ============================================================================
 */
package custom.opensource.cpi.sdia.smart.kafka;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.zip.GZIPInputStream;

/**
 * Minimal Confluent Schema Registry client with no external JSON dependency.
 * Fetches Avro schema JSON by schema ID using GET /schemas/ids/{id}.
 */
public final class SdiaSchemaRegistryClient {

    private static final class CacheEntry {
        final String schema;
        final int schemaBytes;
        final long expiresAtMs;
        volatile long lastAccessMs;

        CacheEntry(final String schema,
                   final int schemaBytes,
                   final long expiresAtMs,
                   final long lastAccessMs) {
            this.schema = schema;
            this.schemaBytes = schemaBytes;
            this.expiresAtMs = expiresAtMs;
            this.lastAccessMs = lastAccessMs;
        }

        boolean isExpired(final long nowMs) {
            return nowMs > expiresAtMs;
        }

        void touch(final long nowMs) {
            this.lastAccessMs = nowMs;
        }
    }

    private static final Map<String, CacheEntry> SCHEMA_CACHE = new ConcurrentHashMap<String, CacheEntry>();
    private static final Map<String, Object> FETCH_LOCKS = new ConcurrentHashMap<String, Object>();
    private static final Object CACHE_EVICTION_LOCK = new Object();

    private static final int MAX_SCHEMA_CACHE_SIZE = 512;
    /** Schema cache TTL: 1 hour. Confluent Schema Registry IDs are immutable. */
    private static final long CACHE_TTL_MS = 3_600_000L;
    private static final int DEFAULT_MAX_SCHEMA_BYTES = 200 * 1024;
    private static final int MAX_ERROR_BODY_BYTES = 4096;
    private static final int CONNECT_TIMEOUT_MS = 3000;
    private static final int READ_TIMEOUT_MS = 8000;

    private SdiaSchemaRegistryClient() { }

    public static String fetchSchema(final String registryUrl,
                                     final int schemaId,
                                     final String apiKey,
                                     final String apiSecret) throws Exception {
        return fetchSchema(registryUrl, schemaId, apiKey, apiSecret, DEFAULT_MAX_SCHEMA_BYTES);
    }

    public static String fetchSchema(final String registryUrl,
                                     final int schemaId,
                                     final String apiKey,
                                     final String apiSecret,
                                     final int maxSchemaBytes) throws Exception {
        return fetchSchema(registryUrl, schemaId, apiKey, apiSecret, maxSchemaBytes, CONNECT_TIMEOUT_MS, READ_TIMEOUT_MS);
    }

    public static String fetchSchema(final String registryUrl,
                                     final int schemaId,
                                     final String apiKey,
                                     final String apiSecret,
                                     final int maxSchemaBytes,
                                     final int connectTimeoutMs,
                                     final int readTimeoutMs) throws Exception {

        final String normalizedRegistryUrl = normalizeUrl(registryUrl);
        if (normalizedRegistryUrl.length() == 0) {
            throw new IllegalArgumentException("Schema Registry host is empty.");
        }
        if (schemaId <= 0) {
            throw new IllegalArgumentException("Schema ID must be positive. schemaId=" + schemaId);
        }

        final int effectiveMaxSchemaBytes = maxSchemaBytes > 0 ? maxSchemaBytes : DEFAULT_MAX_SCHEMA_BYTES;
        final int effectiveConnectTimeoutMs = normalizeTimeoutMs(connectTimeoutMs, CONNECT_TIMEOUT_MS);
        final int effectiveReadTimeoutMs = normalizeTimeoutMs(readTimeoutMs, READ_TIMEOUT_MS);
        final String cacheKey = normalizedRegistryUrl + "#" + schemaId + "#" + credentialFingerprint(apiKey, apiSecret);
        final long now = System.currentTimeMillis();

        final CacheEntry cached = getValidCached(cacheKey, effectiveMaxSchemaBytes, schemaId, normalizedRegistryUrl, now);
        if (cached != null) {
            return cached.schema;
        }

        final Object lock = lockFor(cacheKey);
        try {
            synchronized (lock) {
                final long lockedNow = System.currentTimeMillis();
                final CacheEntry cachedAfterLock = getValidCached(cacheKey, effectiveMaxSchemaBytes, schemaId, normalizedRegistryUrl, lockedNow);
                if (cachedAfterLock != null) {
                    return cachedAfterLock.schema;
                }

                final String endpoint = normalizedRegistryUrl + "/schemas/ids/" + schemaId;
                final HttpURLConnection conn = (HttpURLConnection) new URL(endpoint).openConnection();
                try {
                    conn.setRequestMethod("GET");
                    conn.setRequestProperty("Accept", "application/vnd.schemaregistry.v1+json");
                    conn.setRequestProperty("Connection", "keep-alive");
                    // Do not force gzip. Some Schema Registry endpoints return compressed bytes;
                    // readBodyLimited() still handles Content-Encoding defensively if a proxy/server sends gzip.
                    conn.setUseCaches(false);
                    conn.setConnectTimeout(effectiveConnectTimeoutMs);
                    conn.setReadTimeout(effectiveReadTimeoutMs);

                    if (apiKey != null && apiKey.length() > 0) {
                        final String credentials = apiKey + ":" + (apiSecret != null ? apiSecret : "");
                        final String encoded = Base64.getEncoder().encodeToString(credentials.getBytes(StandardCharsets.UTF_8));
                        conn.setRequestProperty("Authorization", "Basic " + encoded);
                    }

                    final int httpCode = conn.getResponseCode();
                    if (httpCode != HttpURLConnection.HTTP_OK) {
                        final String errorBody = readBodyLimited(conn.getErrorStream(), MAX_ERROR_BODY_BYTES, conn.getContentEncoding());
                        final String diagnostic;
                        if (httpCode == HttpURLConnection.HTTP_NOT_FOUND) {
                            diagnostic = "Schema Registry schema not found. schemaId=" + schemaId
                                    + " registry=" + normalizedRegistryUrl
                                    + ". For Magic Byte mode, this schemaId was extracted from Kafka payload header bytes 1-4. "
                                    + "Verify that the producer embedded the correct Confluent Schema Registry ID for this registry, not the subject version and not a schema id from another environment.";
                        } else if (httpCode == HttpURLConnection.HTTP_UNAUTHORIZED || httpCode == HttpURLConnection.HTTP_FORBIDDEN) {
                            diagnostic = "Schema Registry authentication/authorization failed. http=" + httpCode
                                    + " registry=" + normalizedRegistryUrl
                                    + ". Verify the CPI security material/credential alias and Schema Registry API key permissions.";
                        } else {
                            diagnostic = "Schema Registry request failed. http=" + httpCode
                                    + " schemaId=" + schemaId
                                    + " registry=" + normalizedRegistryUrl;
                        }
                        throw new RuntimeException(diagnostic
                                + " endpoint=" + endpoint
                                + (errorBody.length() > 0 ? " body=" + errorBody : ""));
                    }

                    final String body = readBodyLimited(conn.getInputStream(), effectiveMaxSchemaBytes + MAX_ERROR_BODY_BYTES, conn.getContentEncoding());
                    final String schemaJson = extractSchemaField(body);
                    final int schemaBytes = utf8Length(schemaJson);
                    validateSchemaSize(schemaBytes, effectiveMaxSchemaBytes, schemaId, normalizedRegistryUrl);
                    putCacheEntry(cacheKey, new CacheEntry(schemaJson, schemaBytes,
                            System.currentTimeMillis() + CACHE_TTL_MS, System.currentTimeMillis()));
                    return schemaJson;
                } finally {
                    conn.disconnect();
                }
            }
        } finally {
            FETCH_LOCKS.remove(cacheKey, lock);
        }
    }

    public static void clearCache() {
        SCHEMA_CACHE.clear();
    }

    private static CacheEntry getValidCached(final String cacheKey,
                                             final int effectiveMaxSchemaBytes,
                                             final int schemaId,
                                             final String normalizedRegistryUrl,
                                             final long nowMs) {
        final CacheEntry cached = SCHEMA_CACHE.get(cacheKey);
        if (cached == null) {
            return null;
        }
        if (cached.isExpired(nowMs)) {
            SCHEMA_CACHE.remove(cacheKey, cached);
            return null;
        }
        validateSchemaSize(cached.schemaBytes, effectiveMaxSchemaBytes, schemaId, normalizedRegistryUrl);
        cached.touch(nowMs);
        return cached;
    }

    private static Object lockFor(final String cacheKey) {
        final Object created = new Object();
        final Object existing = FETCH_LOCKS.putIfAbsent(cacheKey, created);
        return existing == null ? created : existing;
    }

    private static void putCacheEntry(final String key, final CacheEntry entry) {
        synchronized (CACHE_EVICTION_LOCK) {
            evictExpired(System.currentTimeMillis());
            while (SCHEMA_CACHE.size() >= MAX_SCHEMA_CACHE_SIZE) {
                if (!evictOldest()) {
                    break;
                }
            }
            SCHEMA_CACHE.put(key, entry);
        }
    }

    private static void evictExpired(final long nowMs) {
        for (Map.Entry<String, CacheEntry> e : SCHEMA_CACHE.entrySet()) {
            final CacheEntry value = e.getValue();
            if (value == null || value.isExpired(nowMs)) {
                SCHEMA_CACHE.remove(e.getKey(), value);
            }
        }
    }

    private static boolean evictOldest() {
        String oldestKey = null;
        CacheEntry oldest = null;
        for (Map.Entry<String, CacheEntry> e : SCHEMA_CACHE.entrySet()) {
            final CacheEntry value = e.getValue();
            if (value == null) {
                continue;
            }
            if (oldest == null || value.lastAccessMs < oldest.lastAccessMs) {
                oldest = value;
                oldestKey = e.getKey();
            }
        }
        return oldestKey != null && SCHEMA_CACHE.remove(oldestKey, oldest);
    }

    private static String readBody(final InputStream stream) throws Exception {
        return readBodyLimited(stream, DEFAULT_MAX_SCHEMA_BYTES, null);
    }

    private static String readBodyLimited(final InputStream stream,
                                          final int maxBytes,
                                          final String contentEncoding) throws Exception {
        if (stream == null) {
            return "";
        }
        final int effectiveMax = maxBytes > 0 ? maxBytes : DEFAULT_MAX_SCHEMA_BYTES;
        final StringBuilder body = new StringBuilder(Math.min(16384, effectiveMax));
        final BufferedInputStream buffered = stream instanceof BufferedInputStream
                ? (BufferedInputStream) stream
                : new BufferedInputStream(stream, 8192);

        InputStream source = buffered;
        if (isGzipEncoded(buffered, contentEncoding)) {
            source = new GZIPInputStream(buffered, 8192);
        }

        final BufferedReader reader = new BufferedReader(new InputStreamReader(source, StandardCharsets.UTF_8), 8192);
        int readChars = 0;
        try {
            final char[] buffer = new char[8192];
            int n;
            while ((n = reader.read(buffer)) != -1) {
                readChars += n;
                if (readChars > effectiveMax) {
                    throw new RuntimeException("Schema Registry response exceeded configured size limit of "
                            + effectiveMax + " bytes/chars.");
                }
                body.append(buffer, 0, n);
            }
        } finally {
            try { reader.close(); } catch (Exception ignored) { }
        }
        return body.toString();
    }

    private static boolean isGzipEncoded(final BufferedInputStream source,
                                         final String contentEncoding) throws Exception {
        if (contentEncoding != null && contentEncoding.toLowerCase().indexOf("gzip") >= 0) {
            return true;
        }

        source.mark(2);
        final int b0 = source.read();
        final int b1 = source.read();
        source.reset();

        return (b0 & 0xFF) == 0x1F && (b1 & 0xFF) == 0x8B;
    }

    private static void validateSchemaSize(final int schemaBytes,
                                           final int maxSchemaBytes,
                                           final int schemaId,
                                           final String registryUrl) {
        if (schemaBytes <= 0) {
            throw new RuntimeException("Schema Registry returned an empty schema. schemaId=" + schemaId);
        }
        if (schemaBytes > maxSchemaBytes) {
            throw new RuntimeException("Schema Registry schema too large. schemaId=" + schemaId
                    + " schemaBytes=" + schemaBytes
                    + " configuredLimitBytes=" + maxSchemaBytes
                    + " registry=" + registryUrl);
        }
    }

    private static int utf8Length(final String value) {
        if (value == null || value.length() == 0) {
            return 0;
        }
        int bytes = 0;
        for (int i = 0; i < value.length(); i++) {
            final char c = value.charAt(i);
            if (c <= 0x7F) {
                bytes += 1;
            } else if (c <= 0x7FF) {
                bytes += 2;
            } else if (Character.isHighSurrogate(c) && i + 1 < value.length()
                    && Character.isLowSurrogate(value.charAt(i + 1))) {
                bytes += 4;
                i++;
            } else {
                bytes += 3;
            }
        }
        return bytes;
    }

    /** Extracts the string value of the top-level "schema" property from Schema Registry response JSON. */
    private static String extractSchemaField(final String responseJson) {
        if (responseJson == null) {
            throw new RuntimeException("Empty Schema Registry response");
        }
        final int keyPos = findJsonStringKey(responseJson, "schema");
        if (keyPos < 0) {
            throw new RuntimeException("Cannot find 'schema' field in Schema Registry response: " + responseJson);
        }
        final int colon = responseJson.indexOf(':', keyPos);
        if (colon < 0) {
            throw new RuntimeException("Invalid Schema Registry response, missing ':' after schema field");
        }
        int i = colon + 1;
        while (i < responseJson.length() && Character.isWhitespace(responseJson.charAt(i))) {
            i++;
        }
        if (i >= responseJson.length() || responseJson.charAt(i) != '"') {
            throw new RuntimeException("Invalid Schema Registry response, schema field is not a JSON string");
        }
        return parseJsonString(responseJson, i).value;
    }

    private static int findJsonStringKey(final String json, final String key) {
        final String quoted = "\"" + key + "\"";
        int pos = -1;
        while ((pos = json.indexOf(quoted, pos + 1)) >= 0) {
            int i = pos + quoted.length();
            while (i < json.length() && Character.isWhitespace(json.charAt(i))) {
                i++;
            }
            if (i < json.length() && json.charAt(i) == ':') {
                return pos;
            }
        }
        return -1;
    }

    static ParsedString parseJsonString(final String json, final int quoteIndex) {
        if (quoteIndex < 0 || quoteIndex >= json.length() || json.charAt(quoteIndex) != '"') {
            throw new IllegalArgumentException("JSON string must start with quote at index " + quoteIndex);
        }
        final StringBuilder out = new StringBuilder();
        int i = quoteIndex + 1;
        while (i < json.length()) {
            final char c = json.charAt(i++);
            if (c == '"') {
                return new ParsedString(out.toString(), i);
            }
            if (c == '\\') {
                if (i >= json.length()) {
                    throw new IllegalArgumentException("Invalid JSON escape at end of string");
                }
                final char e = json.charAt(i++);
                switch (e) {
                    case '"': out.append('"'); break;
                    case '\\': out.append('\\'); break;
                    case '/': out.append('/'); break;
                    case 'b': out.append('\b'); break;
                    case 'f': out.append('\f'); break;
                    case 'n': out.append('\n'); break;
                    case 'r': out.append('\r'); break;
                    case 't': out.append('\t'); break;
                    case 'u':
                        if (i + 4 > json.length()) {
                            throw new IllegalArgumentException("Invalid JSON unicode escape");
                        }
                        final int code = Integer.parseInt(json.substring(i, i + 4), 16);
                        out.append((char) code);
                        i += 4;
                        break;
                    default:
                        throw new IllegalArgumentException("Invalid JSON escape: \\" + e);
                }
            } else {
                out.append(c);
            }
        }
        throw new IllegalArgumentException("Unterminated JSON string");
    }

    static final class ParsedString {
        final String value;
        final int nextIndex;
        ParsedString(final String value, final int nextIndex) {
            this.value = value;
            this.nextIndex = nextIndex;
        }
    }

    private static int normalizeTimeoutMs(final int value, final int fallback) {
        if (value < 1000) return fallback;
        if (value > 300000) return 300000;
        return value;
    }

    private static String credentialFingerprint(final String apiKey, final String apiSecret) {
        if ((apiKey == null || apiKey.length() == 0) && (apiSecret == null || apiSecret.length() == 0)) {
            return "anonymous";
        }
        final String principal = (apiKey == null ? "" : apiKey) + ':' + (apiSecret == null ? "" : apiSecret);
        return Integer.toHexString(principal.hashCode());
    }

    private static String normalizeUrl(final String url) {
        if (url == null) {
            return "";
        }
        String value = url.trim();
        if (value.length() == 0) {
            return "";
        }

        final int schemasIdx = value.indexOf("/schemas/");
        if (schemasIdx > 0) {
            value = value.substring(0, schemasIdx);
        }
        final int subjectsIdx = value.indexOf("/subjects/");
        if (subjectsIdx > 0) {
            value = value.substring(0, subjectsIdx);
        }

        if (!value.startsWith("http://") && !value.startsWith("https://")) {
            value = "https://" + value;
        }
        while (value.endsWith("/")) {
            value = value.substring(0, value.length() - 1);
        }
        return value;
    }
}
