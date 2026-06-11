/*
 * ============================================================================
 * Event Smart Kafka Adapter — SDIA
 * ============================================================================
 * Copyright (c) 2026 Ricardo Luz Holanda Viana
 * Dual-Licensed: Apache License 2.0 / MIT License
 * ⚠️  This header must NOT be removed or altered in any distribution.
 * ============================================================================
 */
package custom.opensource.cpi.sdia.smart.kafka;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Minimal Confluent / Karapace Schema Registry client.
 *
 * <h3>Caching strategy</h3>
 * Avro schema IDs are immutable in the Schema Registry: once a schema is
 * registered under an ID, that ID always maps to the same JSON. Therefore a
 * long TTL is safe and correct.
 *
 * <ul>
 *   <li><b>Schema JSON cache</b> — holds the raw JSON returned by the registry.
 *       Default TTL: 1 hour. Configurable via {@link #SCHEMA_CACHE_TTL_MS}.</li>
 *   <li><b>Parsed schema cache</b> — holds the {@link SdiaKafkaAvroConverter.CachedSchema}
 *       built from the JSON. Parsing a schema with 30 fields allocates ~35 objects;
 *       caching the result makes every subsequent message allocation-free for schema
 *       resolution. Same TTL as the JSON cache.</li>
 *   <li><b>LRU eviction</b> — entries beyond {@link #MAX_SCHEMA_CACHE_ENTRIES} are
 *       evicted least-recently-used, not all-at-once. A bulk {@code clear()} would
 *       cause 256 simultaneous cache-miss HTTP requests — catastrophic for large
 *       schemas.</li>
 * </ul>
 *
 * <h3>Schema size</h3>
 * The caller passes {@code maxSchemaBytes} to guard against accidentally fetching
 * enormous schemas. The supported range is 1 B – 100 KB; values outside this range
 * are clamped. 50 KB is the default, 100 KB is the maximum.
 */
public final class SdiaSchemaRegistryClient {

    /**
     * Default TTL for schema cache entries: 1 hour.
     * Schema IDs in Confluent / Karapace Schema Registry are immutable — once a
     * schema is registered under an ID, that ID always maps to the same JSON.
     * A 1-hour TTL is therefore safe and correct for both Magic Byte and Fixed
     * Schema ID modes.
     *
     * The effective TTL is set per call via the {@code ttlMs} parameter in the
     * overloaded {@link #fetchParsedSchema} methods. The default is used when the
     * caller does not specify a TTL (e.g. direct API calls without an endpoint context).
     */
    static final long SCHEMA_CACHE_TTL_MS_DEFAULT = 3_600_000L; // 1 hour
    static final long SCHEMA_CACHE_TTL_MS_2H      = 7_200_000L; // 2 hours
    /** @deprecated Use SCHEMA_CACHE_TTL_MS_DEFAULT. Retained for binary compatibility. */
    @Deprecated
    static final long SCHEMA_CACHE_TTL_MS         = SCHEMA_CACHE_TTL_MS_DEFAULT;

    static final int  MAX_SCHEMA_CACHE_ENTRIES = 256;

    // Hard limits for schema JSON size.
    // Valid channel values from the metadata dropdown: 1, 50, 75, 100 KB.
    // 200 KB was listed in an earlier metadata.xml draft but is NOT supported —
    // the ceiling has always been 100 KB. The metadata option has been removed.
    static final int  MIN_SCHEMA_BYTES       = 1;
    static final int  MAX_SCHEMA_BYTES       = 100 * 1024; // 100 KB absolute ceiling
    static final int  DEFAULT_SCHEMA_BYTES   = 50  * 1024; // 50 KB default

    private static final int CONNECT_TIMEOUT_MS  = 5_000;
    private static final int READ_TIMEOUT_MS     = 10_000;
    private static final int MAX_ERROR_BODY_BYTES = 4_096;

    /**
     * LRU map guarded by a ReadWriteLock.
     * Multiple threads can read the cache concurrently (cache hit path).
     * Writes (cache miss, eviction, clear) acquire the write lock exclusively.
     * In the typical single-consumer-per-iFlow SAP CPI deployment, contention is
     * near-zero; the ReadWriteLock eliminates theoretical blocking on future
     * multi-threaded deployments with zero cost on the single-threaded fast path.
     */
    private static final Map<String, SchemaEntry> CACHE = new LinkedHashMap<String, SchemaEntry>(
            MAX_SCHEMA_CACHE_ENTRIES + 1, 0.75f, true /* access-order = LRU */) {
        @Override
        protected boolean removeEldestEntry(final Map.Entry<String, SchemaEntry> eldest) {
            return size() > MAX_SCHEMA_CACHE_ENTRIES;
        }
    };
    private static final ReadWriteLock CACHE_RW_LOCK   = new ReentrantReadWriteLock();
    private static final java.util.concurrent.locks.Lock CACHE_READ_LOCK  = CACHE_RW_LOCK.readLock();
    private static final java.util.concurrent.locks.Lock CACHE_WRITE_LOCK = CACHE_RW_LOCK.writeLock();

    private SdiaSchemaRegistryClient() {}

    // -------------------------------------------------------------------------
    // Public API
    // -------------------------------------------------------------------------

    public static String fetchSchema(final String registryUrl,
                                     final int schemaId,
                                     final String apiKey,
                                     final String apiSecret) throws Exception {
        return fetchSchema(registryUrl, schemaId, apiKey, apiSecret, DEFAULT_SCHEMA_BYTES);
    }

    /**
     * Returns the Avro schema JSON for the given {@code schemaId} using the
     * default 1-hour TTL. Results are cached for {@link #SCHEMA_CACHE_TTL_MS_DEFAULT}.
     *
     * @param maxSchemaBytes upper bound on schema JSON size; clamped to [1, 100 KB].
     */
    public static String fetchSchema(final String registryUrl,
                                     final int schemaId,
                                     final String apiKey,
                                     final String apiSecret,
                                     final int maxSchemaBytes) throws Exception {
        return fetchSchema(registryUrl, schemaId, apiKey, apiSecret,
                maxSchemaBytes, SCHEMA_CACHE_TTL_MS_DEFAULT);
    }

    /**
     * Returns the Avro schema JSON for the given {@code schemaId}.
     * Results are cached for {@code ttlMs} milliseconds.
     *
     * @param maxSchemaBytes upper bound on schema JSON size; clamped to [1, 100 KB].
     * @param ttlMs          cache TTL in milliseconds; positive value required.
     */
    public static String fetchSchema(final String registryUrl,
                                     final int schemaId,
                                     final String apiKey,
                                     final String apiSecret,
                                     final int maxSchemaBytes,
                                     final long ttlMs) throws Exception {
        final String normalizedUrl  = normalizeUrl(registryUrl);
        if (normalizedUrl.isEmpty()) throw new IllegalArgumentException("Schema Registry URL is empty.");
        if (schemaId <= 0)           throw new IllegalArgumentException("Schema ID must be positive. schemaId=" + schemaId);

        final long effectiveTtlMs   = (ttlMs > 0) ? ttlMs : SCHEMA_CACHE_TTL_MS_DEFAULT;
        final int  effectiveMaxBytes = clampSchemaBytes(maxSchemaBytes);
        final String cacheKey = normalizedUrl + '#' + schemaId;
        final long now = System.currentTimeMillis();

        // ── Fast path — cache hit (read lock) ────────────────────────────────
        CACHE_READ_LOCK.lock();
        try {
            final SchemaEntry entry = CACHE.get(cacheKey);
            if (entry != null && (now - entry.cachedAtMs) < effectiveTtlMs) {
                validateSchemaSize(entry.schemaJson, effectiveMaxBytes, schemaId, normalizedUrl);
                return entry.schemaJson;
            }
        } finally {
            CACHE_READ_LOCK.unlock();
        }

        // ── Cache miss — fetch from registry (outside lock) ───────────────────
        final String schemaJson = fetchFromRegistry(normalizedUrl, schemaId, apiKey, apiSecret,
                effectiveMaxBytes);

        CACHE_WRITE_LOCK.lock();
        try {
            evictExpired(now, effectiveTtlMs);
            CACHE.put(cacheKey, new SchemaEntry(schemaJson, now));
        } finally {
            CACHE_WRITE_LOCK.unlock();
        }
        return schemaJson;
    }

    /**
     * Returns the pre-parsed {@link SdiaKafkaAvroConverter.CachedSchema} for the
     * given schema ID, building and caching it on the first call.
     *
     * Parsing a schema with 30+ fields allocates ~35 objects. Caching eliminates
     * that allocation on every subsequent message.
     */
    /**
     * Returns the pre-parsed {@link SdiaKafkaAvroConverter.CachedSchema} using the
     * default 1-hour TTL.
     */
    public static SdiaKafkaAvroConverter.CachedSchema fetchParsedSchema(
            final String registryUrl,
            final int schemaId,
            final String apiKey,
            final String apiSecret,
            final int maxSchemaBytes) throws Exception {
        return fetchParsedSchema(registryUrl, schemaId, apiKey, apiSecret,
                maxSchemaBytes, SCHEMA_CACHE_TTL_MS_DEFAULT);
    }

    /**
     * Returns the pre-parsed {@link SdiaKafkaAvroConverter.CachedSchema} for the
     * given schema ID, building and caching it on the first call.
     *
     * <h3>Cache design</h3>
     * Parsing a schema with 30+ fields allocates ~35 objects. Caching the result
     * eliminates that allocation on every subsequent message for the same schema ID.
     * Both Magic Byte and Fixed Schema ID modes benefit equally — the cache key is
     * {@code registryUrl#schemaId} regardless of how the ID was resolved.
     *
     * @param ttlMs cache TTL in milliseconds. Use {@link #SCHEMA_CACHE_TTL_MS_DEFAULT}
     *              (1h) or {@link #SCHEMA_CACHE_TTL_MS_2H} (2h).
     *              The 2-hour TTL halves Schema Registry round-trips during long-running
     *              deployments. Safe because Schema Registry IDs are immutable.
     */
    public static SdiaKafkaAvroConverter.CachedSchema fetchParsedSchema(
            final String registryUrl,
            final int schemaId,
            final String apiKey,
            final String apiSecret,
            final int maxSchemaBytes,
            final long ttlMs) throws Exception {

        final String normalizedUrl = normalizeUrl(registryUrl);
        if (normalizedUrl.isEmpty()) throw new IllegalArgumentException("Schema Registry URL is empty.");
        if (schemaId <= 0)           throw new IllegalArgumentException("Schema ID must be positive. schemaId=" + schemaId);

        final long effectiveTtlMs   = (ttlMs > 0) ? ttlMs : SCHEMA_CACHE_TTL_MS_DEFAULT;
        final int  effectiveMaxBytes = clampSchemaBytes(maxSchemaBytes);
        final String cacheKey = normalizedUrl + '#' + schemaId;
        final long now = System.currentTimeMillis();

        // ── Fast path — cache hit (read lock, concurrent-safe) ───────────────
        CACHE_READ_LOCK.lock();
        try {
            final SchemaEntry entry = CACHE.get(cacheKey);
            if (entry != null && entry.parsedSchema != null
                    && (now - entry.cachedAtMs) < effectiveTtlMs) {
                return entry.parsedSchema;
            }
        } finally {
            CACHE_READ_LOCK.unlock();
        }

        // ── Cache miss — fetch JSON (may reuse JSON-only cache entry) ─────────
        final String schemaJson = fetchSchema(normalizedUrl, schemaId, apiKey, apiSecret,
                effectiveMaxBytes, effectiveTtlMs);

        // ── Parse outside lock — pure CPU work ────────────────────────────────
        final SdiaKafkaAvroConverter.CachedSchema parsed =
                SdiaKafkaAvroConverter.parseSchema(schemaJson);

        CACHE_WRITE_LOCK.lock();
        try {
            SchemaEntry entry = CACHE.get(cacheKey);
            if (entry == null) {
                entry = new SchemaEntry(schemaJson, now);
            }
            entry.parsedSchema = parsed;
            CACHE.put(cacheKey, entry);
        } finally {
            CACHE_WRITE_LOCK.unlock();
        }
        return parsed;
    }

    /** Clears all cached entries. Called on adapter redeploy. */
    public static void clearCache() {
        CACHE_WRITE_LOCK.lock();
        try { CACHE.clear(); } finally { CACHE_WRITE_LOCK.unlock(); }
    }

    // -------------------------------------------------------------------------
    // Internal helpers
    // -------------------------------------------------------------------------

    private static String fetchFromRegistry(final String normalizedUrl,
                                            final int schemaId,
                                            final String apiKey,
                                            final String apiSecret,
                                            final int maxSchemaBytes) throws Exception {
        final String endpoint = normalizedUrl + "/schemas/ids/" + schemaId;
        final HttpURLConnection conn = (HttpURLConnection) new URL(endpoint).openConnection();
        try {
            conn.setRequestMethod("GET");
            conn.setRequestProperty("Accept", "application/vnd.schemaregistry.v1+json");
            conn.setConnectTimeout(CONNECT_TIMEOUT_MS);
            conn.setReadTimeout(READ_TIMEOUT_MS);
            conn.setUseCaches(false);

            if (apiKey != null && !apiKey.isEmpty()) {
                final String credentials = apiKey + ':' + (apiSecret != null ? apiSecret : "");
                conn.setRequestProperty("Authorization",
                        "Basic " + Base64.getEncoder().encodeToString(
                                credentials.getBytes(StandardCharsets.UTF_8)));
            }

            final int httpCode = conn.getResponseCode();
            if (httpCode != HttpURLConnection.HTTP_OK) {
                final String errorBody = readBodyLimited(conn.getErrorStream(), MAX_ERROR_BODY_BYTES);
                final String diagnostic;
                if (httpCode == HttpURLConnection.HTTP_NOT_FOUND) {
                    diagnostic = "Schema Registry schema not found. schemaId=" + schemaId
                            + " registry=" + normalizedUrl
                            + ". For Magic Byte mode, this ID was extracted from the Kafka payload header bytes 1-4."
                            + " Verify the producer embedded the correct Confluent Schema Registry ID.";
                } else if (httpCode == HttpURLConnection.HTTP_UNAUTHORIZED || httpCode == HttpURLConnection.HTTP_FORBIDDEN) {
                    diagnostic = "Schema Registry authentication/authorization failed. http=" + httpCode
                            + " registry=" + normalizedUrl
                            + ". Verify the CPI security material credential alias and Schema Registry API key permissions.";
                } else {
                    diagnostic = "Schema Registry request failed. http=" + httpCode
                            + " schemaId=" + schemaId
                            + " registry=" + normalizedUrl;
                }
                throw new RuntimeException(diagnostic + " endpoint=" + endpoint
                        + (errorBody.isEmpty() ? "" : " body=" + errorBody));
            }

            final String body = readBodyLimited(conn.getInputStream(), maxSchemaBytes + MAX_ERROR_BODY_BYTES);
            final String schemaJson = extractSchemaField(body);
            validateSchemaSize(schemaJson, maxSchemaBytes, schemaId, normalizedUrl);
            return schemaJson;
        } finally {
            conn.disconnect();
        }
    }

    private static void evictExpired(final long now, final long ttlMs) {
        final Iterator<Map.Entry<String, SchemaEntry>> it = CACHE.entrySet().iterator();
        while (it.hasNext()) {
            if ((now - it.next().getValue().cachedAtMs) >= ttlMs) it.remove();
        }
    }

    private static void validateSchemaSize(final String schemaJson,
                                           final int maxBytes,
                                           final int schemaId,
                                           final String registryUrl) {
        if (schemaJson == null || schemaJson.isEmpty()) {
            throw new RuntimeException("Schema Registry returned empty schema. schemaId=" + schemaId
                    + " registry=" + registryUrl);
        }
        final int byteLength = schemaJson.getBytes(StandardCharsets.UTF_8).length;
        if (byteLength > maxBytes) {
            throw new RuntimeException("Schema JSON exceeds the configured limit of " + maxBytes
                    + " bytes. actual=" + byteLength
                    + " schemaId=" + schemaId
                    + " registry=" + registryUrl
                    + ". Increase schemaIdBufferSize in the adapter channel (max 100 KB).");
        }
    }

    private static int clampSchemaBytes(final int requested) {
        if (requested < MIN_SCHEMA_BYTES) return DEFAULT_SCHEMA_BYTES;
        if (requested > MAX_SCHEMA_BYTES) return MAX_SCHEMA_BYTES;
        return requested;
    }

    private static String readBodyLimited(final java.io.InputStream stream,
                                          final int maxBytes) {
        if (stream == null) return "";
        try (final BufferedReader reader =
                     new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
            final StringBuilder sb = new StringBuilder(Math.min(maxBytes, 4096));
            int totalRead = 0;
            String line;
            while ((line = reader.readLine()) != null) {
                totalRead += line.length();
                if (totalRead > maxBytes) break;
                sb.append(line);
            }
            return sb.toString();
        } catch (final Exception ignored) {
            return "";
        }
    }

    /**
     * Extracts the value of the {@code "schema"} field from the Schema Registry
     * JSON response without pulling in a JSON library.
     * The response format is: {@code {"schemaType":"AVRO","schema":"<escaped-json>",...}}
     */
    private static String extractSchemaField(final String body) {
        if (body == null) return "";
        final String key = "\"schema\"";
        int keyIdx = body.indexOf(key);
        if (keyIdx < 0) return body; // unexpected format — return as-is
        int colonIdx = body.indexOf(':', keyIdx + key.length());
        if (colonIdx < 0) return body;
        int valStart = colonIdx + 1;
        while (valStart < body.length() && body.charAt(valStart) <= ' ') valStart++;
        if (valStart >= body.length() || body.charAt(valStart) != '"') return body;
        valStart++; // skip opening quote
        final StringBuilder sb = new StringBuilder(body.length() - valStart);
        int pos = valStart;
        while (pos < body.length()) {
            final char c = body.charAt(pos);
            if (c == '"') break;
            if (c == '\\' && pos + 1 < body.length()) {
                final char next = body.charAt(pos + 1);
                if      (next == '"')  { sb.append('"');  pos += 2; }
                else if (next == '\\') { sb.append('\\'); pos += 2; }
                else if (next == 'n')  { sb.append('\n'); pos += 2; }
                else if (next == 'r')  { sb.append('\r'); pos += 2; }
                else if (next == 't')  { sb.append('\t'); pos += 2; }
                else { sb.append(c); pos++; }
            } else {
                sb.append(c);
                pos++;
            }
        }
        return sb.toString();
    }

    private static String normalizeUrl(final String url) {
        if (url == null) return "";
        String u = url.trim();
        while (u.endsWith("/")) u = u.substring(0, u.length() - 1);
        return u;
    }

    // -------------------------------------------------------------------------
    // Cache entry
    // -------------------------------------------------------------------------

    static final class SchemaEntry {
        final String schemaJson;
        final long   cachedAtMs;
        /** Populated lazily on first Avro conversion request for this schema ID. */
        volatile SdiaKafkaAvroConverter.CachedSchema parsedSchema;

        SchemaEntry(final String schemaJson, final long cachedAtMs) {
            this.schemaJson = schemaJson;
            this.cachedAtMs = cachedAtMs;
        }
    }
}
