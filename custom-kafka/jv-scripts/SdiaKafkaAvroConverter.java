/*
 * ============================================================================
 * Event Smart Kafka Adapter
 * SDIA — Semantic Domain Integration Architecture
 * ============================================================================
 *
 * Copyright (c) 2026 Ricardo Luz Holanda Viana
 * Independent Solo Researcher | Enterprise Integration Architecture
 *
 * Dual-Licensed under Apache License 2.0 or MIT License.
 * ============================================================================
 */
package custom.opensource.cpi.sdia.smart.kafka;

import org.apache.camel.Exchange;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Zero-external-dependency Confluent Avro converter for CPI/OSGi-safe deployment.
 * Designed for SAP CPI/OSGi where adding the full Avro stack may be undesirable.
 */
public final class SdiaKafkaAvroConverter {

    private static final byte MAGIC_BYTE = 0x00;
    private static final int HEADER_LENGTH = 5;

    private static final String XML_HEADER = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>";
    private static final String JSON_NULL = "null";
    private static final char[] HEX = "0123456789ABCDEF".toCharArray();

    private static final int MAX_PARSED_SCHEMA_CACHE_SIZE = 512;
    private static final long MAX_COLLECTION_ITEMS = 1_000_000L;
    private static final long MAX_ZERO_BYTE_COLLECTION_ITEMS = 65_536L;
    private static final int MAX_INITIAL_COLLECTION_CAPACITY = 16_384;
    private static final int MAX_STRING_BUILDER_CAPACITY = 64 * 1024 * 1024;
    private static final Map<String, ParsedSchemaEntry> PARSED_SCHEMA_CACHE =
            new ConcurrentHashMap<String, ParsedSchemaEntry>();
    private static final Map<String, Object> PARSED_SCHEMA_LOCKS =
            new ConcurrentHashMap<String, Object>();
    private static final Object PARSED_SCHEMA_EVICTION_LOCK = new Object();

    private SdiaKafkaAvroConverter() { }

    public static String convert(final byte[] rawBytes,
                                 final String format,
                                 final String schemaRegistryUrl,
                                 final String apiKey,
                                 final String apiSecret) throws Exception {
        return convert(rawBytes, format, schemaRegistryUrl, apiKey, apiSecret, true, null);
    }

    public static String convert(final byte[] rawBytes,
                                 final String format,
                                 final String schemaRegistryUrl,
                                 final String apiKey,
                                 final String apiSecret,
                                 final boolean schemaIdFromMagicByte,
                                 final Integer fixedSchemaId) throws Exception {
        return convert(rawBytes, format, schemaRegistryUrl, apiKey, apiSecret,
                schemaIdFromMagicByte, fixedSchemaId, 250 * 1024);
    }

    public static String convert(final byte[] rawBytes,
                                 final String format,
                                 final String schemaRegistryUrl,
                                 final String apiKey,
                                 final String apiSecret,
                                 final boolean schemaIdFromMagicByte,
                                 final Integer fixedSchemaId,
                                 final int maxSchemaBytes) throws Exception {
        return convert(rawBytes, format, schemaRegistryUrl, apiKey, apiSecret,
                schemaIdFromMagicByte, fixedSchemaId, maxSchemaBytes, 5000, 10000);
    }

    public static String convert(final byte[] rawBytes,
                                 final String format,
                                 final String schemaRegistryUrl,
                                 final String apiKey,
                                 final String apiSecret,
                                 final boolean schemaIdFromMagicByte,
                                 final Integer fixedSchemaId,
                                 final int maxSchemaBytes,
                                 final int schemaRegistryConnectTimeoutMs,
                                 final int schemaRegistryReadTimeoutMs) throws Exception {

        final int schemaId;
        final int decoderStartOffset;

        if (schemaIdFromMagicByte) {
            validateConfluentHeader(rawBytes);
            schemaId = extractSchemaId(rawBytes);
            decoderStartOffset = HEADER_LENGTH;
        } else {
            if (fixedSchemaId == null || fixedSchemaId.intValue() <= 0) {
                throw new IllegalArgumentException("Fixed Schema ID must be a positive number.");
            }
            validatePayload(rawBytes);
            schemaId = fixedSchemaId.intValue();

            // Fixed Schema ID mode has two valid payload shapes:
            // 1) raw Avro binary without a Confluent header -> decode from byte 0;
            // 2) Confluent wire format payload -> skip 0x00 + 4-byte schema id,
            //    but only when the wire schema id matches the fixed channel schema id.
            //
            // Without this explicit check, a payload encoded with schema A and decoded with
            // fixed schema B fails later with misleading errors such as
            // "Invalid Avro string length: -34". Failing here gives the user the real cause.
            if (hasConfluentHeader(rawBytes)) {
                final int wireSchemaId = extractSchemaId(rawBytes);
                if (wireSchemaId != schemaId) {
                    throw new IllegalArgumentException("Fixed Schema ID mismatch. Channel Fixed Schema ID="
                            + schemaId + ", but payload Confluent wire header Schema ID=" + wireSchemaId
                            + ". Re-encode the payload with schema ID " + schemaId
                            + ", change the channel Fixed Schema ID to " + wireSchemaId
                            + ", or use Schema ID Source = Magic Byte.");
                }
                decoderStartOffset = HEADER_LENGTH;
            } else if (looksLikeBase64Text(rawBytes)) {
                throw new IllegalArgumentException("Payload appears to be Base64 text, not Avro binary. "
                        + "Kafka record value first bytes hex=" + firstBytesHex(rawBytes, 16)
                        + ", asciiPreview=\"" + asciiPreview(rawBytes, 32) + "\". "
                        + "Decode Base64 before producing to Kafka or produce the .bin file as raw binary bytes. "
                        + "Fixed Schema ID mode accepts raw Avro binary or Confluent wire-format binary, not literal Base64 text.");
            } else {
                decoderStartOffset = 0;
            }
        }

        final String schemaJson = SdiaSchemaRegistryClient.fetchSchema(
                schemaRegistryUrl,
                schemaId,
                apiKey,
                apiSecret,
                maxSchemaBytes,
                schemaRegistryConnectTimeoutMs,
                schemaRegistryReadTimeoutMs
        );

        final ParsedSchemaEntry parsedSchema = parsedSchema(schemaRegistryUrl, schemaId, schemaJson);
        final AvroSchema schema = parsedSchema.schema;
        final AvroDecoder decoder = new AvroDecoder(rawBytes, decoderStartOffset);
        final Object datum = readValue(schema, decoder);
        decoder.ensureFullyConsumed();

        // Para JSON: Avro binary expande tipicamente 2-4x. Para XML: 3-5x por tags.
        // Usar 4x para JSON e 5x para XML evita resize em >95% dos payloads reais.
        final boolean xmlOutput = "XML".equalsIgnoreCase(format);
        final int estimatedCapacity = estimateOutputCapacity(rawBytes.length, xmlOutput);

        if (xmlOutput) {
            return buildXml(parsedSchema.rootName, datum, estimatedCapacity);
        }
        return buildJson(datum, estimatedCapacity);
    }

    public static int extractSchemaId(final byte[] rawBytes) {
        validateConfluentHeader(rawBytes);
        return ((rawBytes[1] & 0xFF) << 24)
                | ((rawBytes[2] & 0xFF) << 16)
                | ((rawBytes[3] & 0xFF) << 8)
                | (rawBytes[4] & 0xFF);
    }

    private static void validateConfluentHeader(final byte[] rawBytes) {
        if (rawBytes == null || rawBytes.length < HEADER_LENGTH) {
            throw new IllegalArgumentException("Payload too short for Confluent Avro header: "
                    + (rawBytes == null ? 0 : rawBytes.length) + " bytes");
        }
        if (rawBytes[0] != MAGIC_BYTE) {
            throw new IllegalArgumentException("Missing Confluent Avro magic byte. Got: 0x"
                    + toHex(rawBytes[0]));
        }
    }

    private static void validatePayload(final byte[] rawBytes) {
        if (rawBytes == null || rawBytes.length == 0) {
            throw new IllegalArgumentException("Payload is empty/null. Cannot decode Avro.");
        }
    }

    private static boolean hasConfluentHeader(final byte[] rawBytes) {
        return rawBytes != null && rawBytes.length >= HEADER_LENGTH && rawBytes[0] == MAGIC_BYTE;
    }

    private static boolean looksLikeBase64Text(final byte[] rawBytes) {
        if (rawBytes == null || rawBytes.length < 8) {
            return false;
        }

        final int limit = Math.min(rawBytes.length, 96);
        int base64Chars = 0;
        int nonWhitespace = 0;

        for (int i = 0; i < limit; i++) {
            final int c = rawBytes[i] & 0xFF;
            if (c == ' ' || c == '\r' || c == '\n' || c == '\t') {
                continue;
            }
            nonWhitespace++;
            if ((c >= 'A' && c <= 'Z')
                    || (c >= 'a' && c <= 'z')
                    || (c >= '0' && c <= '9')
                    || c == '+'
                    || c == '/'
                    || c == '=') {
                base64Chars++;
            } else {
                return false;
            }
        }

        return nonWhitespace >= 8 && base64Chars == nonWhitespace;
    }

    private static String firstBytesHex(final byte[] rawBytes, final int max) {
        if (rawBytes == null || rawBytes.length == 0) {
            return "EMPTY";
        }
        final int limit = Math.min(rawBytes.length, Math.max(1, max));
        final StringBuilder out = new StringBuilder(limit * 3);
        for (int i = 0; i < limit; i++) {
            if (i > 0) out.append(' ');
            out.append(toHex(rawBytes[i]));
        }
        return out.toString();
    }

    private static String asciiPreview(final byte[] rawBytes, final int max) {
        if (rawBytes == null || rawBytes.length == 0) {
            return "";
        }
        final int limit = Math.min(rawBytes.length, Math.max(1, max));
        final StringBuilder out = new StringBuilder(limit);
        for (int i = 0; i < limit; i++) {
            final int c = rawBytes[i] & 0xFF;
            if (c >= 32 && c <= 126) {
                out.append((char) c);
            } else {
                out.append('.');
            }
        }
        return out.toString();
    }

    private static ParsedSchemaEntry parsedSchema(final String schemaRegistryUrl,
                                                  final int schemaId,
                                                  final String schemaJson) {
        final int schemaHash = schemaJson == null ? 0 : schemaJson.hashCode();
        final String key = String.valueOf(schemaRegistryUrl) + '#' + schemaId;
        final long now = System.currentTimeMillis();

        final ParsedSchemaEntry cached = PARSED_SCHEMA_CACHE.get(key);
        if (cached != null && cached.schemaHash == schemaHash) {
            cached.touch(now);
            return cached;
        }

        final Object lock = parsedSchemaLockFor(key);
        try {
            synchronized (lock) {
                final ParsedSchemaEntry cachedAfterLock = PARSED_SCHEMA_CACHE.get(key);
                if (cachedAfterLock != null && cachedAfterLock.schemaHash == schemaHash) {
                    cachedAfterLock.touch(System.currentTimeMillis());
                    return cachedAfterLock;
                }

                final Object schemaObject = new JsonParser(schemaJson).parse();
                final SchemaContext context = new SchemaContext();
                final AvroSchema schema = AvroSchema.from(schemaObject, context);
                final String rootName = (schema.name != null && !schema.name.isEmpty()) ? schema.name : "record";
                final ParsedSchemaEntry parsed = new ParsedSchemaEntry(schema, rootName, schemaHash, System.currentTimeMillis());
                putParsedSchema(key, parsed);
                return parsed;
            }
        } finally {
            PARSED_SCHEMA_LOCKS.remove(key, lock);
        }
    }

    private static Object parsedSchemaLockFor(final String key) {
        final Object created = new Object();
        final Object existing = PARSED_SCHEMA_LOCKS.putIfAbsent(key, created);
        return existing == null ? created : existing;
    }

    private static void putParsedSchema(final String key, final ParsedSchemaEntry entry) {
        synchronized (PARSED_SCHEMA_EVICTION_LOCK) {
            while (PARSED_SCHEMA_CACHE.size() >= MAX_PARSED_SCHEMA_CACHE_SIZE) {
                if (!evictOldestParsedSchema()) {
                    break;
                }
            }
            PARSED_SCHEMA_CACHE.put(key, entry);
        }
    }

    private static boolean evictOldestParsedSchema() {
        String oldestKey = null;
        ParsedSchemaEntry oldest = null;
        for (Map.Entry<String, ParsedSchemaEntry> e : PARSED_SCHEMA_CACHE.entrySet()) {
            final ParsedSchemaEntry value = e.getValue();
            if (value == null) {
                continue;
            }
            if (oldest == null || value.lastAccessMs < oldest.lastAccessMs) {
                oldest = value;
                oldestKey = e.getKey();
            }
        }
        return oldestKey != null && PARSED_SCHEMA_CACHE.remove(oldestKey, oldest);
    }

    private static final class ParsedSchemaEntry {
        final AvroSchema schema;
        final String rootName;
        final int schemaHash;
        volatile long lastAccessMs;

        ParsedSchemaEntry(final AvroSchema schema,
                          final String rootName,
                          final int schemaHash,
                          final long lastAccessMs) {
            this.schema = schema;
            this.rootName = rootName;
            this.schemaHash = schemaHash;
            this.lastAccessMs = lastAccessMs;
        }

        void touch(final long nowMs) {
            this.lastAccessMs = nowMs;
        }
    }

    private static Object readValue(final AvroSchema schema, final AvroDecoder decoder) {
        if (schema == null) {
            throw new IllegalArgumentException("Schema is null");
        }
        switch (schema.type) {
            case RECORD:
                final int size = schema.fields.size();
                // Inicialização com carga matemática exata para mitigar re-hashing do Map
                final LinkedHashMap<String, Object> record = new LinkedHashMap<>((int) ((size + 1) / 0.75f) + 1, 0.75f);
                for (int i = 0; i < size; i++) {
                    final AvroField field = schema.fields.get(i);
                    record.put(field.name, readValue(field.schema, decoder));
                }
                return record;
            case STRING:
                return decoder.readString();
            case INT:
                return decoder.readInt("int");
            case LONG:
                return decoder.readLong();
            case FLOAT:
                return decoder.readFloat();
            case DOUBLE:
                return decoder.readDouble();
            case BOOLEAN:
                return decoder.readBoolean() ? Boolean.TRUE : Boolean.FALSE;
            case NULL:
                return null;
            case ENUM:
                final int enumIndex = decoder.readInt("enum index");
                if (enumIndex < 0 || enumIndex >= schema.symbols.size()) {
                    throw new IllegalArgumentException("Invalid Avro enum index " + enumIndex + " for " + schema.name);
                }
                return schema.symbols.get(enumIndex);
            case ARRAY:
                final int arrayMinItemBytes = minEncodedSize(schema.elementType);
                long count = decoder.readLong();
                final ArrayList<Object> list = new ArrayList<Object>(boundedInitialCollectionCapacity(count));
                long totalArrayItems = 0L;
                while (count != 0) {
                    final long blockCount = normalizeCollectionBlockCount(count, "array");
                    if (count < 0) {
                        final long blockBytes = decoder.readLong();
                        validateAvroBlockSize(blockBytes, decoder.remaining(), "array");
                        validateCollectionEncodingDensity(blockCount, blockBytes, arrayMinItemBytes, "array");
                    }
                    totalArrayItems = checkedCollectionCount(totalArrayItems, blockCount, "array", arrayMinItemBytes, decoder.remaining());
                    ensureArrayCapacity(list, totalArrayItems);
                    for (long i = 0; i < blockCount; i++) {
                        list.add(readValue(schema.elementType, decoder));
                    }
                    count = decoder.readLong();
                }
                return list;
            case MAP:
                final int mapMinItemBytes = minEncodedSize(schema.valueType) + 1;
                long mapCount = decoder.readLong();
                final LinkedHashMap<String, Object> map = new LinkedHashMap<String, Object>(boundedHashCapacity(mapCount), 0.75f);
                long totalMapItems = 0L;
                while (mapCount != 0) {
                    final long blockCount = normalizeCollectionBlockCount(mapCount, "map");
                    if (mapCount < 0) {
                        final long blockBytes = decoder.readLong();
                        validateAvroBlockSize(blockBytes, decoder.remaining(), "map");
                        validateCollectionEncodingDensity(blockCount, blockBytes, mapMinItemBytes, "map");
                    }
                    totalMapItems = checkedCollectionCount(totalMapItems, blockCount, "map", mapMinItemBytes, decoder.remaining());
                    for (long i = 0; i < blockCount; i++) {
                        map.put(decoder.readString(), readValue(schema.valueType, decoder));
                    }
                    mapCount = decoder.readLong();
                }
                return map;
            case BYTES:
                return decoder.readBytes();
            case FIXED:
                return decoder.readFixed(schema.size);
            case UNION:
                final int branchIndex = decoder.readInt("union branch index");
                if (branchIndex < 0 || branchIndex >= schema.branches.size()) {
                    throw new IllegalArgumentException("Invalid Avro union branch index " + branchIndex);
                }
                return readValue(schema.branches.get(branchIndex), decoder);
            default:
                throw new IllegalArgumentException("Unsupported Avro schema type: " + schema.type);
        }
    }

    private static int estimateOutputCapacity(final int payloadLength, final boolean xmlOutput) {
        final long multiplier = xmlOutput ? 5L : 4L;
        final long estimated = Math.max(2048L, ((long) payloadLength) * multiplier);
        return (int) Math.min(estimated, MAX_STRING_BUILDER_CAPACITY);
    }

    private static long checkedCollectionCount(final long currentTotal,
                                               final long blockCount,
                                               final String type,
                                               final int minItemBytes,
                                               final int remainingBytes) {
        final long maxLogicalItems = (minItemBytes == 0) ? MAX_ZERO_BYTE_COLLECTION_ITEMS : MAX_COLLECTION_ITEMS;
        if (blockCount < 0L || blockCount > maxLogicalItems) {
            throw new IllegalArgumentException("Invalid Avro " + type + " block count: " + blockCount);
        }
        if (minItemBytes > 0) {
            final long maxByRemaining = remainingBytes / (long) minItemBytes;
            if (blockCount > maxByRemaining) {
                throw new IllegalArgumentException("Invalid Avro " + type + " block count " + blockCount
                        + ": minimum encoded size exceeds remaining payload bytes. minItemBytes="
                        + minItemBytes + ", remainingPayloadBytes=" + remainingBytes);
            }
        }
        final long nextTotal = currentTotal + blockCount;
        if (nextTotal < currentTotal || nextTotal > maxLogicalItems) {
            throw new IllegalArgumentException("Avro " + type + " item count exceeds safety limit: " + nextTotal);
        }
        return nextTotal;
    }

    private static long normalizeCollectionBlockCount(final long rawCount,
                                                      final String type) {
        if (rawCount == Long.MIN_VALUE) {
            throw new IllegalArgumentException("Invalid Avro " + type + " block count: " + rawCount);
        }
        return rawCount < 0L ? -rawCount : rawCount;
    }

    private static void validateCollectionEncodingDensity(final long blockCount,
                                                          final long blockBytes,
                                                          final int minItemBytes,
                                                          final String type) {
        if (minItemBytes <= 0 || blockCount <= 0L) {
            return;
        }
        final long requiredBytes = blockCount * (long) minItemBytes;
        if (requiredBytes < 0L || requiredBytes > blockBytes) {
            throw new IllegalArgumentException("Invalid Avro " + type + " block density: count=" + blockCount
                    + ", blockBytes=" + blockBytes + ", minimumRequiredBytes=" + requiredBytes);
        }
    }

    private static void validateAvroBlockSize(final long blockBytes,
                                              final int remainingBytes,
                                              final String type) {
        if (blockBytes < 0L || blockBytes > remainingBytes) {
            throw new IllegalArgumentException("Invalid Avro " + type + " block byte size: "
                    + blockBytes + ", remainingPayloadBytes=" + remainingBytes);
        }
    }

    private static int boundedInitialCollectionCapacity(final long rawBlockCount) {
        final long blockCount = normalizeCollectionBlockCount(rawBlockCount, "collection");
        if (blockCount <= 0L) {
            return 16;
        }
        final long bounded = Math.min(blockCount, (long) MAX_INITIAL_COLLECTION_CAPACITY);
        return (int) Math.max(16L, bounded);
    }

    private static int boundedHashCapacity(final long rawBlockCount) {
        final long expected = Math.min((long) Integer.MAX_VALUE / 2L, (long) boundedInitialCollectionCapacity(rawBlockCount));
        return (int) Math.min((expected * 4L + 2L) / 3L, (long) Integer.MAX_VALUE - 8L);
    }

    private static void ensureArrayCapacity(final ArrayList<Object> list, final long totalItems) {
        if (list == null || totalItems <= list.size()) {
            return;
        }
        final long bounded = Math.min(totalItems, (long) MAX_INITIAL_COLLECTION_CAPACITY);
        if (bounded > list.size()) {
            list.ensureCapacity((int) bounded);
        }
    }

    private static int minEncodedSize(final AvroSchema schema) {
        if (schema == null || schema.type == null) {
            return 1;
        }
        switch (schema.type) {
            case NULL:
                return 0;
            case BOOLEAN:
            case INT:
            case LONG:
            case ENUM:
            case STRING:
            case BYTES:
                return 1;
            case FLOAT:
                return 4;
            case DOUBLE:
                return 8;
            case FIXED:
                return schema.size < 0 ? 0 : schema.size;
            case RECORD:
                int total = 0;
                final int fieldCount = schema.fields.size();
                for (int i = 0; i < fieldCount; i++) {
                    total += minEncodedSize(schema.fields.get(i).schema);
                }
                return total;
            case ARRAY:
            case MAP:
                return 1;
            case UNION:
                int min = Integer.MAX_VALUE;
                final int branchCount = schema.branches.size();
                for (int i = 0; i < branchCount; i++) {
                    final int branchMin = minEncodedSize(schema.branches.get(i));
                    if (branchMin < min) {
                        min = branchMin;
                    }
                }
                return 1 + (min == Integer.MAX_VALUE ? 0 : min);
            default:
                return 1;
        }
    }

    private static String buildJson(final Object value, final int initialCapacity) {
        final StringBuilder out = new StringBuilder(initialCapacity);
        appendJson(out, value);
        return out.toString();
    }

    @SuppressWarnings("unchecked")
    private static void appendJson(final StringBuilder out, final Object value) {
        if (value == null) {
            out.append(JSON_NULL);
        } else if (value instanceof String) {
            out.append('"');
            escapeJson(out, (String) value);
            out.append('"');
        } else if (value instanceof Number || value instanceof Boolean) {
            out.append(value.toString());
        } else if (value instanceof byte[]) {
            out.append('"').append(Base64.getEncoder().encodeToString((byte[]) value)).append('"');
        } else if (value instanceof Map) {
            out.append('{');
            boolean first = true;
            for (Map.Entry<String, Object> e : ((Map<String, Object>) value).entrySet()) {
                if (!first) out.append(',');
                first = false;
                out.append('"');
                escapeJson(out, e.getKey());
                out.append("\":");
                appendJson(out, e.getValue());
            }
            out.append('}');
        } else if (value instanceof List) {
            out.append('[');
            final List<?> list = (List<?>) value;
            final int size = list.size();
            for (int i = 0; i < size; i++) {
                if (i > 0) out.append(',');
                appendJson(out, list.get(i));
            }
            out.append(']');
        } else {
            out.append('"');
            escapeJson(out, value.toString());
            out.append('"');
        }
    }

    private static String buildXml(final String rootName, final Object value, final int initialCapacity) {
        final StringBuilder out = new StringBuilder(initialCapacity + 64);
        out.append(XML_HEADER);
        appendXml(out, toXmlName(rootName), value);
        return out.toString();
    }

    @SuppressWarnings("unchecked")
    private static void appendXml(final StringBuilder out, final String name, final Object value) {
        final String safeName = toXmlName(name);
        if (value == null) {
            out.append('<').append(safeName).append("/>");
        } else if (value instanceof Map) {
            out.append('<').append(safeName).append('>');
            for (Map.Entry<String, Object> e : ((Map<String, Object>) value).entrySet()) {
                appendXml(out, e.getKey(), e.getValue());
            }
            out.append("</").append(safeName).append('>');
        } else if (value instanceof List) {
            out.append('<').append(safeName).append('>');
            final List<?> list = (List<?>) value;
            final int size = list.size();
            for (int i = 0; i < size; i++) {
                appendXml(out, "item", list.get(i));
            }
            out.append("</").append(safeName).append('>');
        } else if (value instanceof byte[]) {
            out.append('<').append(safeName).append('>')
                    .append(Base64.getEncoder().encodeToString((byte[]) value))
                    .append("</").append(safeName).append('>');
        } else {
            out.append('<').append(safeName).append('>');
            escapeXml(out, value.toString());
            out.append("</").append(safeName).append('>');
        }
    }

    public static String convert(byte[] payload, SdiaKafkaEndpoint endpoint, Exchange exchange) {
        if (endpoint == null) {
            throw new IllegalArgumentException("Endpoint is null.");
        }
        try {
            final String format = endpoint.getEffectiveConversionFormat();
            final boolean magic = endpoint.isSchemaIdFromMagicByte();
            Integer fixedId = null;
            if (!magic) {
                final String id = endpoint.getSchemaRegistrySchemaId();
                if (id == null || id.trim().length() == 0) {
                    throw new IllegalArgumentException("Fixed Schema ID mode requires schemaRegistrySchemaId.");
                }
                fixedId = Integer.valueOf(id.trim());
            }

            final SdiaKafkaCredentials credentials =
                    SdiaKafkaCredentialsResolver.resolve(endpoint.getSchemaRegistryCredentialAlias());

            return convert(payload,
                    format,
                    endpoint.getSchemaRegistryHostAddress(),
                    credentials.getUsername(),
                    credentials.getPassword(),
                    magic,
                    fixedId,
                    endpoint.getSchemaCacheMaxBytes(),
                    endpoint.getSchemaRegistryConnectTimeoutMs().intValue(),
                    endpoint.getSchemaRegistryReadTimeoutMs().intValue());
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalArgumentException(e.getMessage(), e);
        }
    }

    private enum Type {
        RECORD, STRING, INT, LONG, FLOAT, DOUBLE, BOOLEAN, NULL, ENUM, ARRAY, MAP, BYTES, FIXED, UNION
    }

    private static final class AvroField {
        final String name;
        final AvroSchema schema;
        AvroField(String name, AvroSchema schema) {
            this.name = name;
            this.schema = schema;
        }
    }

    private static final class SchemaContext {
        final Map<String, AvroSchema> named = new LinkedHashMap<>();
    }

    @SuppressWarnings("unchecked")
    private static final class AvroSchema {
        Type type;
        String name;
        final List<AvroField> fields = new ArrayList<>();
        final List<String> symbols = new ArrayList<>();
        AvroSchema elementType;
        AvroSchema valueType;
        final List<AvroSchema> branches = new ArrayList<>();
        int size;

        static AvroSchema from(Object node, SchemaContext context) {
            if (node instanceof String) {
                return fromTypeName((String) node, context);
            }
            if (node instanceof List) {
                AvroSchema s = new AvroSchema();
                s.type = Type.UNION;
                final List<?> list = (List<?>) node;
                final int lSize = list.size();
                for (int i = 0; i < lSize; i++) {
                    s.branches.add(from(list.get(i), context));
                }
                return s;
            }
            if (!(node instanceof Map)) {
                throw new IllegalArgumentException("Invalid Avro schema node: " + node);
            }

            final Map<String, Object> map = (Map<String, Object>) node;
            final Object typeNode = map.get("type");
            if (typeNode instanceof List || typeNode instanceof Map) {
                return from(typeNode, context);
            }
            if (!(typeNode instanceof String)) {
                throw new IllegalArgumentException("Invalid Avro schema type: " + typeNode);
            }

            final String typeName = (String) typeNode;
            if (isPrimitive(typeName)) {
                return fromTypeName(typeName, context);
            }
            final AvroSchema cached = context.named.get(typeName);
            if (cached != null) {
                return cached;
            }

            final AvroSchema s = new AvroSchema();
            s.name = stringValue(map.get("name"));

            if ("record".equals(typeName)) {
                s.type = Type.RECORD;
                registerNamed(context, s, stringValue(map.get("namespace")));
                Object fieldsNode = map.get("fields");
                if (!(fieldsNode instanceof List)) {
                    throw new IllegalArgumentException("Avro record has no fields: " + s.name);
                }
                final List<?> fList = (List<?>) fieldsNode;
                final int fSize = fList.size();
                for (int i = 0; i < fSize; i++) {
                    Map<String, Object> fieldMap = (Map<String, Object>) fList.get(i);
                    s.fields.add(new AvroField(stringValue(fieldMap.get("name")), from(fieldMap.get("type"), context)));
                }
                return s;
            }

            if ("enum".equals(typeName)) {
                s.type = Type.ENUM;
                registerNamed(context, s, stringValue(map.get("namespace")));
                Object symbolsNode = map.get("symbols");
                if (symbolsNode instanceof List) {
                    final List<?> symList = (List<?>) symbolsNode;
                    final int symSize = symList.size();
                    for (int i = 0; i < symSize; i++) {
                        s.symbols.add(String.valueOf(symList.get(i)));
                    }
                }
                return s;
            }

            if ("array".equals(typeName)) {
                s.type = Type.ARRAY;
                s.elementType = from(map.get("items"), context);
                return s;
            }

            if ("map".equals(typeName)) {
                s.type = Type.MAP;
                s.valueType = from(map.get("values"), context);
                return s;
            }

            if ("fixed".equals(typeName)) {
                s.type = Type.FIXED;
                s.size = intValue(map.get("size"));
                registerNamed(context, s, stringValue(map.get("namespace")));
                return s;
            }

            throw new IllegalArgumentException("Unsupported or unresolved Avro schema type: " + typeName);
        }

        private static void registerNamed(SchemaContext context, AvroSchema schema, String namespace) {
            if (schema.name == null || schema.name.isEmpty()) {
                return;
            }
            context.named.put(schema.name, schema);
            if (namespace != null && !namespace.isEmpty()) {
                context.named.put(namespace + "." + schema.name, schema);
            }
        }

        private static boolean isPrimitive(String typeName) {
            // Fusão de blocos lógicos por frequência estatística de uso do Avro
            return "string".equals(typeName) || "int".equals(typeName) || "long".equals(typeName)
                    || "boolean".equals(typeName) || "null".equals(typeName) || "bytes".equals(typeName)
                    || "float".equals(typeName) || "double".equals(typeName);
        }

        private static AvroSchema fromTypeName(String typeName, SchemaContext context) {
            AvroSchema named = context.named.get(typeName);
            if (named != null) {
                return named;
            }
            AvroSchema s = new AvroSchema();
            switch (typeName) {
                case "record": s.type = Type.RECORD; break;
                case "string": s.type = Type.STRING; break;
                case "int": s.type = Type.INT; break;
                case "long": s.type = Type.LONG; break;
                case "float": s.type = Type.FLOAT; break;
                case "double": s.type = Type.DOUBLE; break;
                case "boolean": s.type = Type.BOOLEAN; break;
                case "null": s.type = Type.NULL; break;
                case "bytes": s.type = Type.BYTES; break;
                case "enum": s.type = Type.ENUM; break;
                case "array": s.type = Type.ARRAY; break;
                case "map": s.type = Type.MAP; break;
                case "fixed": s.type = Type.FIXED; break;
                default: throw new IllegalArgumentException("Unsupported Avro type reference: " + typeName);
            }
            return s;
        }
    }

    private static final class AvroDecoder {
        private final byte[] data;
        private final int length;
        private int pos;

        AvroDecoder(byte[] data, int offset) {
            if (data == null) {
                throw new IllegalArgumentException("Avro payload is null.");
            }
            if (offset < 0 || offset > data.length) {
                throw new IllegalArgumentException("Invalid Avro decoder offset: " + offset);
            }
            this.data = data;
            this.length = data.length;
            this.pos = offset;
        }

        boolean readBoolean() {
            requireAvailable(1);
            return data[pos++] != 0;
        }

        long readLong() {
            long raw = 0L;
            int shift = 0;
            while (shift <= 63) {
                requireAvailable(1);
                final int b = data[pos++] & 0xFF;
                if (shift == 63 && (b & 0xFE) != 0) {
                    throw new IllegalArgumentException("Invalid Avro long encoding: illegal bits in terminal varint byte.");
                }
                raw |= ((long) (b & 0x7F)) << shift;
                if ((b & 0x80) == 0) {
                    return (raw >>> 1) ^ -(raw & 1L);
                }
                shift += 7;
            }
            throw new IllegalArgumentException("Invalid Avro long encoding: too many continuation bytes.");
        }

        int readInt(final String type) {
            final long value = readLong();
            if (value < Integer.MIN_VALUE || value > Integer.MAX_VALUE) {
                throw new IllegalArgumentException("Invalid Avro " + type + " value out of int range: " + value);
            }
            return (int) value;
        }

        float readFloat() {
            requireAvailable(4);
            final int p = this.pos;
            int bits = (data[p] & 0xFF)
                    | ((data[p + 1] & 0xFF) << 8)
                    | ((data[p + 2] & 0xFF) << 16)
                    | ((data[p + 3] & 0xFF) << 24);
            this.pos = p + 4;
            return Float.intBitsToFloat(bits);
        }

        double readDouble() {
            requireAvailable(8);
            final int p = this.pos;
            long bits = ((long) data[p] & 0xFFL)
                    | (((long) data[p + 1] & 0xFFL) << 8)
                    | (((long) data[p + 2] & 0xFFL) << 16)
                    | (((long) data[p + 3] & 0xFFL) << 24)
                    | (((long) data[p + 4] & 0xFFL) << 32)
                    | (((long) data[p + 5] & 0xFFL) << 40)
                    | (((long) data[p + 6] & 0xFFL) << 48)
                    | (((long) data[p + 7] & 0xFFL) << 56);
            this.pos = p + 8;
            return Double.longBitsToDouble(bits);
        }

        String readString() {
            final int len = readLength("string");
            if (len == 0) return "";
            requireAvailable(len);
            final String s = new String(data, pos, len, StandardCharsets.UTF_8);
            pos += len;
            return s;
        }

        byte[] readBytes() {
            final int len = readLength("bytes");
            // Validar o tamanho contra o buffer ANTES de alocar. Sem isso, um `len`
            // corrompido (até Integer.MAX_VALUE) faz `new byte[len]` tentar reservar
            // gigabytes — GC thrash no worker do CPI e a thread do poll trava sem
            // exception. Com a validação aqui, qualquer corrupção vira uma
            // IllegalArgumentException limpa que cai no markError -> Skip.
            requireAvailable(len);
            final byte[] out = new byte[len];
            if (len > 0) {
                System.arraycopy(data, pos, out, 0, len);
                pos += len;
            }
            return out;
        }

        byte[] readFixed(final int size) {
            if (size < 0) {
                throw new IllegalArgumentException("Invalid Avro fixed size: " + size);
            }
            requireAvailable(size);
            final byte[] out = new byte[size];
            if (size > 0) {
                System.arraycopy(data, pos, out, 0, size);
                pos += size;
            }
            return out;
        }

        private int readLength(final String type) {
            final long len = readLong();
            if (len < 0 || len > Integer.MAX_VALUE) {
                throw new IllegalArgumentException("Invalid Avro " + type + " length: " + len);
            }
            return (int) len;
        }

        void ensureFullyConsumed() {
            if (pos != length) {
                throw new IllegalArgumentException("Avro payload contains trailing bytes after decoding. decodedUntil="
                        + pos + ", payloadLength=" + length + ". Check Schema ID and producer encoding.");
            }
        }

        int remaining() {
            return length - pos;
        }

        private void requireAvailable(final int count) {
            if (count < 0 || count > (length - pos)) {
                throw new IllegalArgumentException("Avro payload ended unexpectedly at byte " + pos
                        + ". Required=" + count + ", available=" + (length - pos));
            }
        }
    }

    private static String toHex(final byte value) {
        final int v = value & 0xFF;
        // Reuse pre-allocated char pair to avoid new char[] on every call
        return String.valueOf(HEX[v >>> 4]) + HEX[v & 0x0F];
    }

    private static final class JsonParser {
        private final String json;
        private int pos;
        private final int length;

        JsonParser(String json) {
            this.json = json == null ? "" : json;
            this.length = this.json.length();
        }

        Object parse() {
            final Object value = parseValue();
            skipWs();
            if (pos != length) {
                throw new IllegalArgumentException("Trailing characters after JSON document at index " + pos);
            }
            return value;
        }

        private Object parseValue() {
            skipWs();
            if (pos >= length) throw new IllegalArgumentException("Unexpected end of JSON");
            final char c = json.charAt(pos);
            if (c == '"') return parseString();
            if (c == '{') return parseObject();
            if (c == '[') return parseArray();
            if (c == 't') { expect("true"); return Boolean.TRUE; }
            if (c == 'f') { expect("false"); return Boolean.FALSE; }
            if (c == 'n') { expect("null"); return null; }
            if (c == '-' || (c >= '0' && c <= '9')) return parseNumber();
            throw new IllegalArgumentException("Unexpected char '" + c + "' at index " + pos);
        }

        private Map<String, Object> parseObject() {
            final LinkedHashMap<String, Object> map = new LinkedHashMap<>();
            pos++;
            skipWs();
            if (consume('}')) return map;
            while (true) {
                skipWs();
                final String key = parseString();
                skipWs();
                expect(':');
                map.put(key, parseValue());
                skipWs();
                if (consume('}')) break;
                expect(',');
            }
            return map;
        }

        private List<Object> parseArray() {
            final ArrayList<Object> list = new ArrayList<>(32);
            pos++;
            skipWs();
            if (consume(']')) return list;
            while (true) {
                list.add(parseValue());
                skipWs();
                if (consume(']')) break;
                expect(',');
            }
            return list;
        }

        private String parseString() {
            pos++;
            final int start = pos;
            final int len = length;
            final String sJson = json;
            while (pos < len) {
                final char c = sJson.charAt(pos);
                if (c == '"') {
                    final String s = sJson.substring(start, pos);
                    pos++;
                    return s;
                }
                if (c == '\\') {
                    return parseStringWithEscapes(start);
                }
                pos++;
            }
            throw new IllegalArgumentException("Unterminated JSON string");
        }

        private String parseStringWithEscapes(int startFrom) {
            final StringBuilder out = new StringBuilder(32);
            out.append(json, startFrom, pos);
            final int len = length;
            final String sJson = json;
            while (pos < len) {
                char c = sJson.charAt(pos++);
                if (c == '"') return out.toString();
                if (c == '\\') {
                    if (pos >= len) {
                        throw new IllegalArgumentException("Invalid escape at end of JSON string");
                    }
                    char e = sJson.charAt(pos++);
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
                            if (pos + 4 > len) {
                                throw new IllegalArgumentException("Invalid JSON unicode escape at index " + pos);
                            }
                            final int code;
                            try {
                                code = Integer.parseInt(sJson.substring(pos, pos + 4), 16);
                            } catch (NumberFormatException nfe) {
                                throw new IllegalArgumentException("Invalid JSON unicode escape at index " + pos, nfe);
                            }
                            out.append((char) code);
                            pos += 4;
                            break;
                        default: throw new IllegalArgumentException("Invalid escape: \\" + e);
                    }
                } else {
                    out.append(c);
                }
            }
            throw new IllegalArgumentException("Unterminated JSON string");
        }

        private Number parseNumber() {
            final int start = pos;
            final int len = length;
            final String sJson = json;
            if (sJson.charAt(pos) == '-') pos++;
            while (pos < len && Character.isDigit(sJson.charAt(pos))) pos++;
            boolean decimal = false;
            if (pos < len && sJson.charAt(pos) == '.') {
                decimal = true;
                pos++;
                while (pos < len && Character.isDigit(sJson.charAt(pos))) pos++;
            }
            if (pos < len && (sJson.charAt(pos) == 'e' || sJson.charAt(pos) == 'E')) {
                decimal = true;
                pos++;
                if (pos < len && (sJson.charAt(pos) == '+' || sJson.charAt(pos) == '-')) pos++;
                while (pos < len && Character.isDigit(sJson.charAt(pos))) pos++;
            }
            final String text = sJson.substring(start, pos);
            return decimal ? Double.valueOf(text) : Long.valueOf(text);
        }

        private void expect(final String text) {
            if (!json.startsWith(text, pos)) throw new IllegalArgumentException("Expected '" + text + "' at " + pos);
            pos += text.length();
        }

        private void expect(final char c) {
            skipWs();
            if (pos >= length || json.charAt(pos) != c) throw new IllegalArgumentException("Expected '" + c + "' at " + pos);
            pos++;
        }

        private boolean consume(final char c) {
            skipWs();
            if (pos < length && json.charAt(pos) == c) {
                pos++;
                return true;
            }
            return false;
        }

        private void skipWs() {
            final int len = length;
            final String sJson = json;
            while (pos < len) {
                final char c = sJson.charAt(pos);
                if (c == ' ' || c == '\t' || c == '\n' || c == '\r') pos++;
                else break;
            }
        }
    }

    private static String stringValue(final Object value) {
        return value == null ? null : String.valueOf(value);
    }

    private static int intValue(final Object value) {
        return (value instanceof Number) ? ((Number) value).intValue() : Integer.parseInt(String.valueOf(value));
    }

    private static void escapeJson(final StringBuilder out, final String s) {
        if (s == null) return;
        final int len = s.length();
        for (int i = 0; i < len; i++) {
            final char c = s.charAt(i);
            switch (c) {
                case '\\': out.append("\\\\"); break;
                case '"': out.append("\\\""); break;
                case '\n': out.append("\\n"); break;
                case '\r': out.append("\\r"); break;
                case '\t': out.append("\\t"); break;
                default: out.append(c);
            }
        }
    }

    private static void escapeXml(final StringBuilder out, final String s) {
        if (s == null) return;
        final int len = s.length();
        for (int i = 0; i < len; i++) {
            final char c = s.charAt(i);
            switch (c) {
                case '&': out.append("&amp;"); break;
                case '<': out.append("&lt;"); break;
                case '>': out.append("&gt;"); break;
                case '"': out.append("&quot;"); break;
                case '\'': out.append("&apos;"); break;
                default: out.append(c);
            }
        }
    }

    private static String toXmlName(final String value) {
        if (value == null || value.isEmpty()) return "field";
        final int len = value.length();
        final StringBuilder out = new StringBuilder(len);
        for (int i = 0; i < len; i++) {
            final char c = value.charAt(i);
            final boolean valid = (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z')
                    || (c >= '0' && c <= '9') || c == '_' || c == '-' || c == '.';
            if (i == 0 && (c >= '0' && c <= '9')) out.append('_');
            out.append(valid ? c : '_');
        }
        return out.toString();
    }
}