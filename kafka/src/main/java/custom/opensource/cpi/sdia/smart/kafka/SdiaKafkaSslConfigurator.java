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
 * Apache License, Version 2.0
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * MIT License
 * https://opensource.org/licenses/MIT
 *
 * You may use this software under either license at your option.
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under these licenses is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * ----------------------------------------------------------------------------
 *
 * ⚠️  NOTICE — This header must NOT be removed or altered in any distribution,
 * derivative work, or reuse of this source code, in whole or in part.
 * Removal of this header constitutes a violation of the license terms
 * and applicable intellectual property rights.
 *
 * ============================================================================
 */
package custom.opensource.cpi.sdia.smart.kafka;

import com.sap.it.api.ITApiFactory;
import com.sap.it.api.exception.InvalidContextException;
import com.sap.it.api.keystore.KeystoreService;
import com.sap.it.api.keystore.exception.KeystoreException;

import java.security.cert.Certificate;
import java.util.Base64;
import java.util.Properties;

/**
 * Kafka TLS trust configurator for the EventSmartKafka adapter.
 *
 * <h3>Design contracts</h3>
 * <ul>
 *   <li>Must only be called when TLS is enabled ({@code security.protocol}
 *       is {@code SSL} or {@code SASL_SSL}). Callers MUST NOT invoke this
 *       class for {@code PLAINTEXT} or {@code SASL_PLAINTEXT}.</li>
 *   <li>Hostname verification is ALWAYS enabled
 *       ({@code ssl.endpoint.identification.algorithm=https}).
 *       This is non-negotiable for production security.</li>
 *   <li>mTLS (client key/certificate) is NOT implemented in v1.0.
 *       {@code ssl.keystore.*} properties are never set by this class.</li>
 *   <li>No "trust-all" / disable-verification mode exists. Debug-only
 *       workarounds must not be added here.</li>
 * </ul>
 *
 * <h3>Trust source dispatch</h3>
 * <pre>
 * JVM_DEFAULT                   → No truststore properties set.
 *                                 JVM built-in CA bundle is used.
 *                                 Correct for Confluent Cloud, Aiven, MSK.
 *
 * PEM_CONTENT                   → ssl.truststore.type = PEM
 *                                 ssl.truststore.certificates = &lt;validated PEM&gt;
 *                                 Correct for self-signed / private-CA brokers
 *                                 (e.g. local Docker Kafka with kafka.server.crt).
 *
 * CPI_TRUSTED_CERTIFICATE_ALIAS → Loads the public certificate from the SAP CPI
 *                                 Keystore by alias, converts to PEM, then sets
 *                                 ssl.truststore.type = PEM and
 *                                 ssl.truststore.certificates = &lt;pem from alias&gt;.
 *                                 The SAP CPI KeystoreService is used; only the
 *                                 public certificate is read (no private key).
 * </pre>
 *
 * <h3>Why PEM in-memory works</h3>
 * Kafka Java client ≥ 2.7 supports {@code ssl.truststore.type=PEM} and
 * {@code ssl.truststore.certificates=<PEM string>} natively. No temporary
 * file, no JKS conversion. The PEM is validated before being passed to the
 * Kafka client.
 */
public final class SdiaKafkaSslConfigurator {

    private SdiaKafkaSslConfigurator() {}

    // =========================================================================
    // Primary entry point
    // =========================================================================

    /**
     * Configures SSL/TLS trust properties on {@code props} based on the
     * {@link SdiaKafkaEndpoint} channel settings.
     *
     * <p>This method MUST only be called when TLS is active (i.e.
     * {@code security.protocol} is {@code SSL} or {@code SASL_SSL}).
     *
     * @param props    the Kafka client properties to mutate (never null)
     * @param endpoint the adapter endpoint carrying channel TLS settings (never null)
     * @throws IllegalArgumentException if PEM content is invalid or missing
     * @throws IllegalStateException    if the CPI Keystore is unavailable or
     *                                  the alias does not exist
     */
    public static void configure(final Properties props, final SdiaKafkaEndpoint endpoint) {
        if (props == null || endpoint == null) {
            return;
        }

        // Hostname verification is mandatory. Always set it — regardless of
        // trust source — so it can never accidentally be left unset.
        props.put("ssl.endpoint.identification.algorithm", "https");

        final String trustSource = endpoint.getTlsTrustSourceEffective();

        if ("JVM_DEFAULT".equals(trustSource)) {
            applyJvmDefaultTrust(props);
        } else if ("PEM_CONTENT".equals(trustSource)) {
            applyPemContentTrust(props, endpoint);
        } else if ("CPI_TRUSTED_CERTIFICATE_ALIAS".equals(trustSource)) {
            applyCpiKeystoreAliasTrust(props, endpoint);
        } else {
            // Unrecognised value: fall back to JVM default and log.
            // This can only happen through direct API misuse, not through the UI.
            applyJvmDefaultTrust(props);
        }
    }

    // =========================================================================
    // Trust source implementations
    // =========================================================================

    /**
     * JVM_DEFAULT: no custom truststore properties are set.
     *
     * <p>The Kafka Java client will use the JVM's built-in CA bundle
     * ({@code $JAVA_HOME/lib/security/cacerts}), which covers all major
     * public CAs used by Confluent Cloud, Aiven, and AWS MSK.
     */
    private static void applyJvmDefaultTrust(final Properties props) {
        // Intentionally empty: JVM default is the Kafka client's own default.
        // Explicitly removing any previously set truststore keys ensures a
        // clean state when this method is called after a Properties merge.
        props.remove("ssl.truststore.type");
        props.remove("ssl.truststore.certificates");
        props.remove("ssl.truststore.location");
        props.remove("ssl.truststore.password");
    }

    /**
     * PEM_CONTENT: validates and sets the PEM certificate inline.
     *
     * <p>Uses the Kafka Java client's native PEM truststore support.
     * No temporary file is created; the validated PEM string is passed
     * directly as {@code ssl.truststore.certificates}.
     */
    private static void applyPemContentTrust(final Properties props,
                                              final SdiaKafkaEndpoint endpoint) {
        final String pem = normalizePem(endpoint.getEffectiveTrustedCaPem());
        props.put("ssl.truststore.type",         "PEM");
        props.put("ssl.truststore.certificates", pem);
    }

    /**
     * CPI_TRUSTED_CERTIFICATE_ALIAS: loads the public certificate from the
     * SAP CPI Keystore by alias, converts it to PEM, and sets the Kafka
     * client's PEM truststore properties.
     *
     * <p>Only the public certificate(s) are read. The private key is never
     * accessed by this method (mTLS is not implemented in v1.0).
     *
     * <p>If the CPI KeystoreService is unavailable or the alias does not
     * resolve to a valid certificate, a clear {@link IllegalStateException}
     * is thrown immediately — there is no silent fallback to JVM default.
     */
    private static void applyCpiKeystoreAliasTrust(final Properties props,
                                                    final SdiaKafkaEndpoint endpoint) {
        final String alias = endpoint.getEffectiveTrustedCertificateAlias();
        if (alias == null || alias.trim().isEmpty()) {
            throw new IllegalArgumentException(
                    "[SDIA Kafka] TLS Trust Source is CPI_TRUSTED_CERTIFICATE_ALIAS "
                    + "but no certificate alias is configured. "
                    + "Set 'tlsTrustedCertificateAlias' (or legacy 'certificateAlias') "
                    + "to a valid SAP CPI Keystore alias.");
        }

        final KeystoreService keystore = getKeystoreService();
        final Certificate[]   certs    = resolveCertificateChainOrSingle(keystore, alias);

        if (certs == null || certs.length == 0) {
            throw new IllegalStateException(
                    "[SDIA Kafka] CPI Keystore alias '" + alias
                    + "' returned no certificates. "
                    + "Verify the alias exists in SAP CPI Security Material → Keystore.");
        }

        final String pem = certificatesToPem(certs);
        props.put("ssl.truststore.type",         "PEM");
        props.put("ssl.truststore.certificates", pem);
    }

    // =========================================================================
    // PEM validation
    // =========================================================================

    /**
     * Validates and normalises a PEM certificate string.
     *
     * <p>Rules enforced:
     * <ul>
     *   <li>Must not be null or blank.</li>
     *   <li>Must NOT contain {@code PRIVATE KEY} in any form
     *       (catches PKCS#8, RSA, EC, encrypted variants).</li>
     *   <li>Must contain both {@code -----BEGIN CERTIFICATE-----} and
     *       {@code -----END CERTIFICATE-----}.</li>
     * </ul>
     *
     * @param pem the raw PEM string from the channel
     * @return the trimmed, validated PEM string
     * @throws IllegalArgumentException if any validation rule is violated
     */
    static String normalizePem(final String pem) {
        if (pem == null || pem.trim().isEmpty()) {
            throw new IllegalArgumentException(
                    "[SDIA Kafka] TLS Trusted CA PEM is required when TLS Trust Source is PEM_CONTENT. "
                    + "Paste the broker CA or server certificate in PEM format.");
        }

        final String normalised = pem.trim();

        // Security check: refuse any value that contains a private key material marker.
        // Check both modern PKCS#8 form and legacy RSA/EC/encrypted forms.
        if (normalised.contains("PRIVATE KEY")) {
            throw new IllegalArgumentException(
                    "[SDIA Kafka] Trusted CA PEM must contain only certificates. "
                    + "Private keys are not allowed. "
                    + "Do not paste key files (*.key, *.pem with private key) into this field.");
        }

        if (!normalised.contains("-----BEGIN CERTIFICATE-----")
                || !normalised.contains("-----END CERTIFICATE-----")) {
            throw new IllegalArgumentException(
                    "[SDIA Kafka] Trusted CA PEM must contain a valid PEM certificate block. "
                    + "Expected format:\n"
                    + "-----BEGIN CERTIFICATE-----\n"
                    + "...(base64)...\n"
                    + "-----END CERTIFICATE-----");
        }

        return normalised;
    }

    // =========================================================================
    // CPI Keystore helpers
    // =========================================================================

    private static Certificate[] resolveCertificateChainOrSingle(final KeystoreService keystore,
                                                                   final String alias) {
        // Try certificate chain first (most common for CA + intermediates).
        try {
            final Certificate[] chain = keystore.getCertificateChain(alias);
            if (chain != null && chain.length > 0) {
                return chain;
            }
        } catch (Throwable ignored) {}

        // Fall back to single certificate lookup.
        try {
            final Certificate certificate = keystore.getCertificate(alias);
            if (certificate != null) {
                return new Certificate[] { certificate };
            }
        } catch (KeystoreException e) {
            throw new IllegalStateException(
                    "[SDIA Kafka] Could not read certificate from SAP CPI Keystore. alias=" + alias, e);
        }
        return new Certificate[0];
    }

    private static KeystoreService getKeystoreService() {
        try {
            final KeystoreService svc = ITApiFactory.getApi(KeystoreService.class, null);
            if (svc == null) {
                throw new IllegalStateException(
                        "[SDIA Kafka] SAP CPI KeystoreService is not available in this runtime. "
                        + "Use TLS Trust Source = PEM_CONTENT or JVM_DEFAULT instead.");
            }
            return svc;
        } catch (InvalidContextException e) {
            throw new IllegalStateException(
                    "[SDIA Kafka] Could not access SAP CPI KeystoreService. "
                    + "Use TLS Trust Source = PEM_CONTENT or JVM_DEFAULT instead.", e);
        }
    }

    // =========================================================================
    // PEM encoding
    // =========================================================================

    /**
     * Converts one or more {@link Certificate} objects to a PEM string.
     *
     * <p>Multiple certificates are concatenated — Kafka's PEM truststore
     * accepts a chain of PEM blocks in a single {@code ssl.truststore.certificates}
     * value.
     */
    private static String certificatesToPem(final Certificate[] certificates) {
        final StringBuilder pem = new StringBuilder(certificates.length * 1600);
        for (final Certificate cert : certificates) {
            if (cert == null) {
                continue;
            }
            try {
                pem.append("-----BEGIN CERTIFICATE-----\n");
                pem.append(Base64.getMimeEncoder(64, new byte[]{'\n'}).encodeToString(cert.getEncoded()));
                pem.append("\n-----END CERTIFICATE-----\n");
            } catch (Exception e) {
                throw new IllegalStateException(
                        "[SDIA Kafka] Could not encode certificate to PEM.", e);
            }
        }
        return pem.toString();
    }
}
