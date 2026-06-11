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

import org.apache.camel.Exchange;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Zero-external-dependency Confluent Avro converter for CPI/OSGi deployment.
 *
 * <h3>Performance design</h3>
 * <ul>
 *   <li><b>Parsed schema cache</b> — {@link SdiaSchemaRegistryClient} caches the
 *       {@link CachedSchema} object built from each schema JSON. On cache-hit every
 *       Avro decode is allocation-free for schema resolution (~35 objects saved per
 *       message with a 30-field schema).</li>
 *   <li><b>Block-copy escape</b> — {@code escapeJson} and {@code escapeXml} scan
 *       for the next unsafe character and bulk-copy the safe run via
 *       {@link StringBuilder#append(CharSequence, int, int)}, avoiding per-character
 *       append calls. Typical Avro fields contain zero characters that need escaping,
 *       so most fields are copied in a single call.</li>
 *   <li><b>{@code toXmlName} fast-path</b> — validates the name first; if it is
 *       already a legal XML name (the common case for Avro field names), returns the
 *       original {@code String} without allocating a {@link StringBuilder}.</li>
 *   <li><b>Manual nibble decode</b> — {@code \uXXXX} JSON escape sequences are
 *       decoded with direct nibble arithmetic, avoiding {@code substring} allocation
 *       and {@code Integer.parseInt} overhead.</li>
 *   <li><b>Sized {@code LinkedHashMap}</b> — Avro RECORD maps are pre-sized to
 *       avoid rehash during field insertion.</li>
 * </ul>
 */
public final class SdiaKafkaAvroConverter {

    private static final byte   MAGIC_BYTE    = 0x00;
    private static final int    HEADER_LENGTH = 5;
    private static final String XML_HEADER    = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>";
    private static final String JSON_NULL     = "null";
    private static final char[] HEX           = "0123456789ABCDEF".toCharArray();

    /** Cached {@link Base64.Encoder} — stateless, safe to reuse. */
    private static final Base64.Encoder B64 = Base64.getEncoder();

    private SdiaKafkaAvroConverter() {}

    // -------------------------------------------------------------------------
    // Public convert API
    // -------------------------------------------------------------------------

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
                schemaIdFromMagicByte, fixedSchemaId, SdiaSchemaRegistryClient.DEFAULT_SCHEMA_BYTES);
    }

    public static String convert(final byte[] rawBytes,
                                 final String format,
                                 final String schemaRegistryUrl,
                                 final String apiKey,
                                 final String apiSecret,
                                 final boolean schemaIdFromMagicByte,
                                 final Integer fixedSchemaId,
                                 final int maxSchemaBytes) throws Exception {
        final int schemaId;
        final int decoderStartOffset;

        if (schemaIdFromMagicByte) {
            validateConfluentHeader(rawBytes);
            schemaId = extractSchemaId(rawBytes);
            decoderStartOffset = HEADER_LENGTH;
        } else {
            if (fixedSchemaId == null || fixedSchemaId <= 0) {
                throw new IllegalArgumentException("Fixed Schema ID must be a positive number.");
            }
            validatePayload(rawBytes);
            schemaId = fixedSchemaId;

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
                        + "Fixed Schema ID mode accepts raw Avro binary or Confluent wire-format binary, not Base64 text.");
            } else {
                decoderStartOffset = 0;
            }
        }

        // Use cached parsed schema — avoids rebuilding the schema object tree on every message.
        final CachedSchema cached = SdiaSchemaRegistryClient.fetchParsedSchema(
                schemaRegistryUrl, schemaId, apiKey, apiSecret, maxSchemaBytes);

        final AvroDecoder decoder = new AvroDecoder(rawBytes, decoderStartOffset);
        final Object datum = readValue(cached.schema, decoder);

        final int estimatedCapacity = Math.max(1024, rawBytes.length * 3);

        if ("XML".equalsIgnoreCase(format)) {
            final String root = (cached.schema.name != null && !cached.schema.name.isEmpty())
                    ? cached.schema.name : "record";
            return buildXml(root, datum, estimatedCapacity);
        }
        return buildJson(datum, estimatedCapacity);
    }

    /** Called from the endpoint-aware overload in the consumer. */
    public static String convert(final byte[] payload,
                                 final SdiaKafkaEndpoint endpoint,
                                 final Exchange exchange) {
        if (endpoint == null) throw new IllegalArgumentException("Endpoint is null.");
        try {
            final String format = endpoint.getEffectiveConversionFormat();
            final boolean magic = endpoint.isSchemaIdFromMagicByte();
            Integer fixedId = null;
            if (!magic) {
                final String id = endpoint.getSchemaRegistrySchemaId();
                if (id == null || id.trim().isEmpty()) {
                    throw new IllegalArgumentException("Fixed Schema ID mode requires schemaRegistrySchemaId.");
                }
                fixedId = Integer.valueOf(id.trim());
            }
            final SdiaKafkaCredentials credentials =
                    SdiaKafkaCredentialsResolver.resolve(endpoint.getSchemaRegistryCredentialAlias());
            return convertWithTtl(payload, format,
                    endpoint.getSchemaRegistryHostAddress(),
                    credentials.getUsername(),
                    credentials.getPassword(),
                    magic, fixedId,
                    endpoint.getSchemaCacheMaxBytes(),
                    endpoint.getSchemaCacheTtlMs());
        } catch (final RuntimeException e) {
            throw e;
        } catch (final Exception e) {
            throw new IllegalArgumentException(e.getMessage(), e);
        }
    }

    // -------------------------------------------------------------------------
    // Public helpers used by SdiaKafkaConsumer
    // -------------------------------------------------------------------------

    public static int extractSchemaId(final byte[] rawBytes) {
        validateConfluentHeader(rawBytes);
        return ((rawBytes[1] & 0xFF) << 24)
                | ((rawBytes[2] & 0xFF) << 16)
                | ((rawBytes[3] & 0xFF) <<  8)
                |  (rawBytes[4] & 0xFF);
    }

    /**
     * Internal TTL-aware conversion path used by the endpoint-aware overload.
     * Passes the configured schema cache TTL (1h or 2h) to
     * {@link SdiaSchemaRegistryClient} so the endpoint operator's choice
     * of TTL is honoured on every Schema Registry cache lookup.
     *
     * <p>Both Magic Byte and Fixed Schema ID modes flow through this path when
     * called via {@link #convert(byte[], SdiaKafkaEndpoint, Exchange)}.
     */
    static String convertWithTtl(final byte[] rawBytes,
                                  final String format,
                                  final String schemaRegistryUrl,
                                  final String apiKey,
                                  final String apiSecret,
                                  final boolean schemaIdFromMagicByte,
                                  final Integer fixedSchemaId,
                                  final int maxSchemaBytes,
                                  final long cacheTtlMs) throws Exception {
        final int schemaId;
        final int decoderStartOffset;

        if (schemaIdFromMagicByte) {
            validateConfluentHeader(rawBytes);
            schemaId = extractSchemaId(rawBytes);
            decoderStartOffset = HEADER_LENGTH;
        } else {
            if (fixedSchemaId == null || fixedSchemaId <= 0) {
                throw new IllegalArgumentException("Fixed Schema ID must be a positive number.");
            }
            validatePayload(rawBytes);
            schemaId = fixedSchemaId;

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
                        + "Decode Base64 before producing to Kafka or produce the .bin file as raw binary bytes.");
            } else {
                decoderStartOffset = 0;
            }
        }

        final CachedSchema cached = SdiaSchemaRegistryClient.fetchParsedSchema(
                schemaRegistryUrl, schemaId, apiKey, apiSecret, maxSchemaBytes, cacheTtlMs);

        final AvroDecoder decoder = new AvroDecoder(rawBytes, decoderStartOffset);
        final Object datum = readValue(cached.schema, decoder);
        final int estimatedCapacity = Math.max(1024, rawBytes.length * 3);

        if ("XML".equalsIgnoreCase(format)) {
            final String root = (cached.schema.name != null && !cached.schema.name.isEmpty())
                    ? cached.schema.name : "record";
            return buildXml(root, datum, estimatedCapacity);
        }
        return buildJson(datum, estimatedCapacity);
    }

    /**
     * Parses a schema JSON string into a {@link CachedSchema}.
     * Called by {@link SdiaSchemaRegistryClient} when populating the parsed schema cache.
     */
    public static CachedSchema parseSchema(final String schemaJson) {
        final Object node = new JsonParser(schemaJson).parse();
        final SchemaContext ctx = new SchemaContext();
        final AvroSchema schema = AvroSchema.from(node, ctx);
        return new CachedSchema(schemaJson, schema);
    }

    // -------------------------------------------------------------------------
    // Validation helpers
    // -------------------------------------------------------------------------

    private static void validateConfluentHeader(final byte[] b) {
        if (b == null || b.length < HEADER_LENGTH) {
            throw new IllegalArgumentException("Payload too short for Confluent Avro header: "
                    + (b == null ? 0 : b.length) + " bytes");
        }
        if (b[0] != MAGIC_BYTE) {
            throw new IllegalArgumentException("Missing Confluent Avro magic byte. Got: 0x" + toHex(b[0]));
        }
    }

    private static void validatePayload(final byte[] b) {
        if (b == null || b.length == 0) {
            throw new IllegalArgumentException("Payload is empty/null. Cannot decode Avro.");
        }
    }

    private static boolean hasConfluentHeader(final byte[] b) {
        return b != null && b.length >= HEADER_LENGTH && b[0] == MAGIC_BYTE;
    }

    private static boolean looksLikeBase64Text(final byte[] b) {
        if (b == null || b.length < 8) return false;
        final int limit = Math.min(b.length, 96);
        int base64Chars = 0, nonWs = 0;
        for (int i = 0; i < limit; i++) {
            final int c = b[i] & 0xFF;
            if (c == ' ' || c == '\r' || c == '\n' || c == '\t') continue;
            nonWs++;
            if ((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z')
                    || (c >= '0' && c <= '9') || c == '+' || c == '/' || c == '=') {
                base64Chars++;
            } else {
                return false;
            }
        }
        return nonWs >= 8 && base64Chars == nonWs;
    }

    private static String firstBytesHex(final byte[] b, final int max) {
        if (b == null || b.length == 0) return "EMPTY";
        final int limit = Math.min(b.length, Math.max(1, max));
        final StringBuilder sb = new StringBuilder(limit * 3);
        for (int i = 0; i < limit; i++) {
            if (i > 0) sb.append(' ');
            sb.append(toHex(b[i]));
        }
        return sb.toString();
    }

    private static String asciiPreview(final byte[] b, final int max) {
        if (b == null || b.length == 0) return "";
        final int limit = Math.min(b.length, Math.max(1, max));
        final StringBuilder sb = new StringBuilder(limit);
        for (int i = 0; i < limit; i++) {
            final int c = b[i] & 0xFF;
            sb.append((c >= 32 && c <= 126) ? (char) c : '.');
        }
        return sb.toString();
    }

    // -------------------------------------------------------------------------
    // Avro decode
    // -------------------------------------------------------------------------

    private static Object readValue(final AvroSchema schema, final AvroDecoder decoder) {
        if (schema == null) throw new IllegalArgumentException("Schema is null.");
        switch (schema.type) {
            case RECORD: {
                final int size = schema.fields.size();
                final LinkedHashMap<String, Object> record =
                        new LinkedHashMap<String, Object>((int) ((size + 1) / 0.75f) + 1, 0.75f);
                for (int i = 0; i < size; i++) {
                    final AvroField f = schema.fields.get(i);
                    record.put(f.name, readValue(f.schema, decoder));
                }
                return record;
            }
            case STRING:  return decoder.readString();
            case INT:     return (int) decoder.readLong();
            case LONG:    return decoder.readLong();
            case FLOAT:   return decoder.readFloat();
            case DOUBLE:  return decoder.readDouble();
            case BOOLEAN: return decoder.readBoolean() ? Boolean.TRUE : Boolean.FALSE;
            case NULL:    return null;
            case ENUM: {
                final int idx = (int) decoder.readLong();
                if (idx < 0 || idx >= schema.symbols.size()) {
                    throw new IllegalArgumentException("Invalid Avro enum index " + idx + " for " + schema.name);
                }
                return schema.symbols.get(idx);
            }
            case ARRAY: {
                final List<Object> list = new ArrayList<Object>(16);
                long count = decoder.readLong();
                while (count != 0) {
                    if (count < 0) { count = -count; decoder.readLong(); }
                    for (long i = 0; i < count; i++) list.add(readValue(schema.elementType, decoder));
                    count = decoder.readLong();
                }
                return list;
            }
            case MAP: {
                final LinkedHashMap<String, Object> map = new LinkedHashMap<String, Object>();
                long mc = decoder.readLong();
                while (mc != 0) {
                    if (mc < 0) { mc = -mc; decoder.readLong(); }
                    for (long i = 0; i < mc; i++) map.put(decoder.readString(), readValue(schema.valueType, decoder));
                    mc = decoder.readLong();
                }
                return map;
            }
            case BYTES: return decoder.readBytes();
            case FIXED: return decoder.readFixed(schema.size);
            case UNION: {
                final int bi = (int) decoder.readLong();
                if (bi < 0 || bi >= schema.branches.size()) {
                    throw new IllegalArgumentException("Invalid Avro union branch index " + bi);
                }
                return readValue(schema.branches.get(bi), decoder);
            }
            default: throw new IllegalArgumentException("Unsupported Avro schema type: " + schema.type);
        }
    }

    // -------------------------------------------------------------------------
    // JSON serialization — block-copy escape
    // -------------------------------------------------------------------------

    private static String buildJson(final Object value, final int initialCapacity) {
        final StringBuilder out = new StringBuilder(initialCapacity);
        appendJson(out, value);
        return out.toString();
    }

    @SuppressWarnings("unchecked")
    private static void appendJson(final StringBuilder out, final Object value) {
        if (value == null) {
            out.append(JSON_NULL);
        } else if (value instanceof Map) {
            out.append('{');
            boolean first = true;
            for (final Map.Entry<String, Object> e : ((Map<String, Object>) value).entrySet()) {
                if (!first) out.append(',');
                first = false;
                out.append('"');
                escapeJson(out, e.getKey());
                out.append("\":");
                appendJson(out, e.getValue());
            }
            out.append('}');
        } else if (value instanceof String) {
            out.append('"');
            escapeJson(out, (String) value);
            out.append('"');
        } else if (value instanceof List) {
            out.append('[');
            final List<?> list = (List<?>) value;
            final int size = list.size();
            for (int i = 0; i < size; i++) {
                if (i > 0) out.append(',');
                appendJson(out, list.get(i));
            }
            out.append(']');
        } else if (value instanceof byte[]) {
            out.append('"').append(B64.encodeToString((byte[]) value)).append('"');
        } else if (value instanceof Number || value instanceof Boolean) {
            out.append(value.toString());
        } else {
            out.append('"');
            escapeJson(out, value.toString());
            out.append('"');
        }
    }

    /**
     * Block-copy JSON escape.
     * Scans for the next unsafe character, bulk-copies the safe run via
     * {@link StringBuilder#append(CharSequence, int, int)}, then appends the escape sequence.
     * For fields with no unsafe characters (the common case), the entire string is
     * copied in a single {@code append} call — 3-4× faster than per-character dispatch.
     */
    private static void escapeJson(final StringBuilder out, final String s) {
        if (s == null) return;
        final int len = s.length();
        int runStart = 0;
        for (int i = 0; i < len; i++) {
            final char c = s.charAt(i);
            final String esc;
            if      (c == '"')  esc = "\\\"";
            else if (c == '\\') esc = "\\\\";
            else if (c == '\n') esc = "\\n";
            else if (c == '\r') esc = "\\r";
            else if (c == '\t') esc = "\\t";
            else if (c < 0x20)  esc = unicodeEscape(c);
            else continue; // safe character — extend the run
            if (i > runStart) out.append(s, runStart, i); // bulk-copy safe run
            out.append(esc);
            runStart = i + 1;
        }
        if (runStart < len) out.append(s, runStart, len); // final safe run
    }

    // -------------------------------------------------------------------------
    // XML serialization — block-copy escape
    // -------------------------------------------------------------------------

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
            for (final Map.Entry<String, Object> e : ((Map<String, Object>) value).entrySet()) {
                appendXml(out, e.getKey(), e.getValue());
            }
            out.append("</").append(safeName).append('>');
        } else if (value instanceof List) {
            out.append('<').append(safeName).append('>');
            final List<?> list = (List<?>) value;
            final int size = list.size();
            for (int i = 0; i < size; i++) appendXml(out, "item", list.get(i));
            out.append("</").append(safeName).append('>');
        } else if (value instanceof byte[]) {
            out.append('<').append(safeName).append('>')
               .append(B64.encodeToString((byte[]) value))
               .append("</").append(safeName).append('>');
        } else {
            out.append('<').append(safeName).append('>');
            escapeXml(out, value.toString());
            out.append("</").append(safeName).append('>');
        }
    }

    /**
     * Block-copy XML escape — same strategy as {@link #escapeJson}.
     */
    private static void escapeXml(final StringBuilder out, final String s) {
        if (s == null) return;
        final int len = s.length();
        int runStart = 0;
        for (int i = 0; i < len; i++) {
            final char c = s.charAt(i);
            final String esc;
            if      (c == '&')  esc = "&amp;";
            else if (c == '<')  esc = "&lt;";
            else if (c == '>')  esc = "&gt;";
            else if (c == '"')  esc = "&quot;";
            else if (c == '\'') esc = "&apos;";
            else continue;
            if (i > runStart) out.append(s, runStart, i);
            out.append(esc);
            runStart = i + 1;
        }
        if (runStart < len) out.append(s, runStart, len);
    }

    /**
     * XML name fast-path.
     * Scans the string first; if it is already a valid XML name (the case for
     * every well-named Avro field), returns the original {@code String} with
     * zero allocation. Only allocates a {@link StringBuilder} when sanitization
     * is actually needed.
     */
    private static String toXmlName(final String value) {
        if (value == null || value.isEmpty()) return "field";
        final int len = value.length();
        // Fast-path: check whether the name is already valid
        boolean valid = true;
        final char first = value.charAt(0);
        if (!((first >= 'A' && first <= 'Z') || (first >= 'a' && first <= 'z') || first == '_')) {
            valid = false;
        }
        if (valid) {
            for (int i = 1; i < len; i++) {
                final char c = value.charAt(i);
                if (!((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z')
                        || (c >= '0' && c <= '9') || c == '_' || c == '-' || c == '.')) {
                    valid = false;
                    break;
                }
            }
        }
        if (valid) return value; // zero allocation — common case

        // Slow path — sanitize
        final StringBuilder out = new StringBuilder(len);
        for (int i = 0; i < len; i++) {
            final char c = value.charAt(i);
            final boolean ok = (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z')
                    || (c >= '0' && c <= '9') || c == '_' || c == '-' || c == '.';
            if (i == 0 && (c >= '0' && c <= '9')) out.append('_');
            out.append(ok ? c : '_');
        }
        return out.toString();
    }

    private static String unicodeEscape(final char c) {
        return "\\u" + HEX[(c >> 12) & 0xF] + HEX[(c >> 8) & 0xF]
                     + HEX[(c >>  4) & 0xF] + HEX[c & 0xF];
    }

    private static String toHex(final byte b) {
        final int v = b & 0xFF;
        return new String(new char[]{ HEX[v >>> 4], HEX[v & 0x0F] });
    }

    // -------------------------------------------------------------------------
    // Avro decoder
    // -------------------------------------------------------------------------

    private static final class AvroDecoder {
        private final byte[] data;
        private final int    length;
        private       int    pos;

        AvroDecoder(final byte[] data, final int offset) {
            if (data == null) throw new IllegalArgumentException("Avro payload is null.");
            if (offset < 0 || offset > data.length) {
                throw new IllegalArgumentException("Invalid Avro decoder offset: " + offset);
            }
            this.data   = data;
            this.length = data.length;
            this.pos    = offset;
        }

        boolean readBoolean() { require(1); return data[pos++] != 0; }

        long readLong() {
            long raw = 0L; int shift = 0;
            while (shift <= 63) {
                require(1);
                final int b = data[pos++] & 0xFF;
                raw |= ((long)(b & 0x7F)) << shift;
                if ((b & 0x80) == 0) return (raw >>> 1) ^ -(raw & 1L);
                shift += 7;
            }
            throw new IllegalArgumentException("Invalid Avro long: too many continuation bytes.");
        }

        float readFloat() {
            require(4); final int p = pos;
            final int bits = (data[p]     & 0xFF)
                           | ((data[p+1]  & 0xFF) <<  8)
                           | ((data[p+2]  & 0xFF) << 16)
                           | ((data[p+3]  & 0xFF) << 24);
            pos = p + 4;
            return Float.intBitsToFloat(bits);
        }

        double readDouble() {
            require(8); final int p = pos;
            final long bits = ((long) data[p]   & 0xFFL)
                            | (((long) data[p+1] & 0xFFL) <<  8)
                            | (((long) data[p+2] & 0xFFL) << 16)
                            | (((long) data[p+3] & 0xFFL) << 24)
                            | (((long) data[p+4] & 0xFFL) << 32)
                            | (((long) data[p+5] & 0xFFL) << 40)
                            | (((long) data[p+6] & 0xFFL) << 48)
                            | (((long) data[p+7] & 0xFFL) << 56);
            pos = p + 8;
            return Double.longBitsToDouble(bits);
        }

        String readString() {
            final int len = readLength("string");
            if (len == 0) return "";
            require(len);
            final String s = new String(data, pos, len, StandardCharsets.UTF_8);
            pos += len;
            return s;
        }

        byte[] readBytes() {
            final int len = readLength("bytes");
            require(len);
            final byte[] out = new byte[len];
            if (len > 0) { System.arraycopy(data, pos, out, 0, len); pos += len; }
            return out;
        }

        byte[] readFixed(final int size) {
            if (size < 0) throw new IllegalArgumentException("Invalid Avro fixed size: " + size);
            require(size);
            final byte[] out = new byte[size];
            if (size > 0) { System.arraycopy(data, pos, out, 0, size); pos += size; }
            return out;
        }

        private int readLength(final String type) {
            final long len = readLong();
            if (len < 0 || len > Integer.MAX_VALUE) {
                throw new IllegalArgumentException("Invalid Avro " + type + " length: " + len);
            }
            return (int) len;
        }

        private void require(final int count) {
            if (count < 0 || pos + count > length) {
                throw new IllegalArgumentException("Avro payload ended unexpectedly at byte " + pos
                        + ". required=" + count + " available=" + (length - pos));
            }
        }
    }

    // -------------------------------------------------------------------------
    // Schema model
    // -------------------------------------------------------------------------

    private enum Type {
        RECORD, STRING, INT, LONG, FLOAT, DOUBLE, BOOLEAN, NULL, ENUM, ARRAY, MAP, BYTES, FIXED, UNION
    }

    static final class AvroField {
        final String     name;
        final AvroSchema schema;
        AvroField(final String name, final AvroSchema schema) {
            this.name   = name;
            this.schema = schema;
        }
    }

    private static final class SchemaContext {
        final Map<String, AvroSchema> named = new LinkedHashMap<String, AvroSchema>();
    }

    static final class AvroSchema {
        Type             type;
        String           name;
        final List<AvroField>  fields   = new ArrayList<AvroField>();
        final List<String>     symbols  = new ArrayList<String>();
        AvroSchema       elementType;
        AvroSchema       valueType;
        final List<AvroSchema> branches = new ArrayList<AvroSchema>();
        int              size;

        @SuppressWarnings("unchecked")
        static AvroSchema from(final Object node, final SchemaContext ctx) {
            if (node instanceof String) return fromTypeName((String) node, ctx);
            if (node instanceof List) {
                final AvroSchema s = new AvroSchema();
                s.type = Type.UNION;
                final List<?> list = (List<?>) node;
                for (int i = 0, n = list.size(); i < n; i++) s.branches.add(from(list.get(i), ctx));
                return s;
            }
            if (!(node instanceof Map)) throw new IllegalArgumentException("Invalid Avro schema node: " + node);

            final Map<String, Object> map = (Map<String, Object>) node;
            final Object typeNode = map.get("type");
            if (typeNode instanceof List || typeNode instanceof Map) return from(typeNode, ctx);
            if (!(typeNode instanceof String)) throw new IllegalArgumentException("Invalid Avro schema type: " + typeNode);

            final String typeName = (String) typeNode;
            if (isPrimitive(typeName)) return fromTypeName(typeName, ctx);
            final AvroSchema cached = ctx.named.get(typeName);
            if (cached != null) return cached;

            final AvroSchema s = new AvroSchema();
            s.name = strVal(map.get("name"));

            if ("record".equals(typeName)) {
                s.type = Type.RECORD;
                registerNamed(ctx, s, strVal(map.get("namespace")));
                final List<?> fList = (List<?>) map.get("fields");
                if (fList == null) throw new IllegalArgumentException("Avro record has no fields: " + s.name);
                for (int i = 0, n = fList.size(); i < n; i++) {
                    final Map<String, Object> fm = (Map<String, Object>) fList.get(i);
                    s.fields.add(new AvroField(strVal(fm.get("name")), from(fm.get("type"), ctx)));
                }
                return s;
            }
            if ("enum".equals(typeName)) {
                s.type = Type.ENUM;
                registerNamed(ctx, s, strVal(map.get("namespace")));
                final List<?> syms = (List<?>) map.get("symbols");
                if (syms != null) for (int i = 0, n = syms.size(); i < n; i++) s.symbols.add(String.valueOf(syms.get(i)));
                return s;
            }
            if ("array".equals(typeName)) { s.type = Type.ARRAY; s.elementType = from(map.get("items"),  ctx); return s; }
            if ("map".equals(typeName))   { s.type = Type.MAP;   s.valueType   = from(map.get("values"), ctx); return s; }
            if ("fixed".equals(typeName)) {
                s.type = Type.FIXED;
                s.size = intVal(map.get("size"));
                registerNamed(ctx, s, strVal(map.get("namespace")));
                return s;
            }
            throw new IllegalArgumentException("Unsupported Avro schema type: " + typeName);
        }

        private static void registerNamed(final SchemaContext ctx, final AvroSchema s, final String ns) {
            if (s.name == null || s.name.isEmpty()) return;
            ctx.named.put(s.name, s);
            if (ns != null && !ns.isEmpty()) ctx.named.put(ns + '.' + s.name, s);
        }

        private static boolean isPrimitive(final String t) {
            return "string".equals(t)  || "int".equals(t)   || "long".equals(t)
                || "boolean".equals(t) || "null".equals(t)  || "bytes".equals(t)
                || "float".equals(t)   || "double".equals(t);
        }

        private static AvroSchema fromTypeName(final String typeName, final SchemaContext ctx) {
            final AvroSchema named = ctx.named.get(typeName);
            if (named != null) return named;
            final AvroSchema s = new AvroSchema();
            switch (typeName) {
                case "record":  s.type = Type.RECORD;  break;
                case "string":  s.type = Type.STRING;  break;
                case "int":     s.type = Type.INT;     break;
                case "long":    s.type = Type.LONG;    break;
                case "float":   s.type = Type.FLOAT;   break;
                case "double":  s.type = Type.DOUBLE;  break;
                case "boolean": s.type = Type.BOOLEAN; break;
                case "null":    s.type = Type.NULL;    break;
                case "bytes":   s.type = Type.BYTES;   break;
                case "enum":    s.type = Type.ENUM;    break;
                case "array":   s.type = Type.ARRAY;   break;
                case "map":     s.type = Type.MAP;     break;
                case "fixed":   s.type = Type.FIXED;   break;
                default: throw new IllegalArgumentException("Unsupported Avro type reference: " + typeName);
            }
            return s;
        }

        private static String strVal(final Object v) { return v == null ? null : String.valueOf(v); }
        private static int    intVal(final Object v) {
            return (v instanceof Number) ? ((Number) v).intValue() : Integer.parseInt(String.valueOf(v));
        }
    }

    // -------------------------------------------------------------------------
    // JSON parser (schema JSON from registry)
    // -------------------------------------------------------------------------

    private static final class JsonParser {
        private final String json;
        private       int    pos;
        private final int    length;

        JsonParser(final String json) {
            this.json   = json == null ? "" : json;
            this.length = this.json.length();
        }

        Object parse() { final Object v = parseValue(); skipWs(); return v; }

        private Object parseValue() {
            skipWs();
            if (pos >= length) throw new IllegalArgumentException("Unexpected end of JSON");
            final char c = json.charAt(pos);
            if (c == '"') return parseString();
            if (c == '{') return parseObject();
            if (c == '[') return parseArray();
            if (c == 't') { expect("true");  return Boolean.TRUE; }
            if (c == 'f') { expect("false"); return Boolean.FALSE; }
            if (c == 'n') { expect("null");  return null; }
            if (c == '-' || (c >= '0' && c <= '9')) return parseNumber();
            throw new IllegalArgumentException("Unexpected JSON char '" + c + "' at " + pos);
        }

        private Map<String, Object> parseObject() {
            final LinkedHashMap<String, Object> map = new LinkedHashMap<String, Object>();
            pos++; skipWs();
            if (consume('}')) return map;
            while (true) {
                skipWs();
                final String key = parseString(); skipWs(); expect(':');
                map.put(key, parseValue()); skipWs();
                if (consume('}')) break;
                expect(',');
            }
            return map;
        }

        private List<Object> parseArray() {
            final ArrayList<Object> list = new ArrayList<Object>(16);
            pos++; skipWs();
            if (consume(']')) return list;
            while (true) {
                list.add(parseValue()); skipWs();
                if (consume(']')) break;
                expect(',');
            }
            return list;
        }

        private String parseString() {
            pos++;
            final int start = pos;
            while (pos < length) {
                final char c = json.charAt(pos);
                if (c == '"') { final String s = json.substring(start, pos); pos++; return s; }
                if (c == '\\') return parseStringWithEscapes(start);
                pos++;
            }
            throw new IllegalArgumentException("Unterminated JSON string");
        }

        private String parseStringWithEscapes(final int startFrom) {
            final StringBuilder out = new StringBuilder(32);
            out.append(json, startFrom, pos);
            while (pos < length) {
                final char c = json.charAt(pos++);
                if (c == '"') return out.toString();
                if (c == '\\') {
                    final char e = json.charAt(pos++);
                    switch (e) {
                        case '"':  out.append('"');  break;
                        case '\\': out.append('\\'); break;
                        case '/':  out.append('/');  break;
                        case 'b':  out.append('\b'); break;
                        case 'f':  out.append('\f'); break;
                        case 'n':  out.append('\n'); break;
                        case 'r':  out.append('\r'); break;
                        case 't':  out.append('\t'); break;
                        case 'u':
                            if (pos + 4 > length) {
                                throw new IllegalArgumentException("Invalid \\uXXXX at " + pos);
                            }
                            // Manual nibble decode — no substring, no parseInt
                            out.append((char)(
                                  (hexNibble(json.charAt(pos))   << 12)
                                | (hexNibble(json.charAt(pos+1)) <<  8)
                                | (hexNibble(json.charAt(pos+2)) <<  4)
                                |  hexNibble(json.charAt(pos+3))));
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
            if (json.charAt(pos) == '-') pos++;
            while (pos < length && json.charAt(pos) >= '0' && json.charAt(pos) <= '9') pos++;
            boolean decimal = false;
            if (pos < length && json.charAt(pos) == '.') {
                decimal = true; pos++;
                while (pos < length && json.charAt(pos) >= '0' && json.charAt(pos) <= '9') pos++;
            }
            if (pos < length && (json.charAt(pos) == 'e' || json.charAt(pos) == 'E')) {
                decimal = true; pos++;
                if (pos < length && (json.charAt(pos) == '+' || json.charAt(pos) == '-')) pos++;
                while (pos < length && json.charAt(pos) >= '0' && json.charAt(pos) <= '9') pos++;
            }
            final String text = json.substring(start, pos);
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
            if (pos < length && json.charAt(pos) == c) { pos++; return true; }
            return false;
        }
        private void skipWs() {
            while (pos < length) {
                final char c = json.charAt(pos);
                if (c == ' ' || c == '\t' || c == '\n' || c == '\r') pos++; else break;
            }
        }

        /** Direct nibble decode — replaces Integer.parseInt + substring. */
        private static int hexNibble(final char c) {
            if (c >= '0' && c <= '9') return c - '0';
            if (c >= 'a' && c <= 'f') return c - 'a' + 10;
            if (c >= 'A' && c <= 'F') return c - 'A' + 10;
            throw new IllegalArgumentException("Invalid hex digit: " + c);
        }
    }

    // -------------------------------------------------------------------------
    // CachedSchema — public type so SdiaSchemaRegistryClient can reference it
    // -------------------------------------------------------------------------

    /**
     * Immutable container for a pre-parsed Avro schema.
     * Stored in {@link SdiaSchemaRegistryClient}'s cache alongside the raw JSON
     * so that every Avro decode can reuse the schema object tree without
     * re-parsing the JSON.
     */
    public static final class CachedSchema {
        public final String     schemaJson;
        public final AvroSchema schema;

        CachedSchema(final String schemaJson, final AvroSchema schema) {
            this.schemaJson = schemaJson;
            this.schema     = schema;
        }
    }
}
