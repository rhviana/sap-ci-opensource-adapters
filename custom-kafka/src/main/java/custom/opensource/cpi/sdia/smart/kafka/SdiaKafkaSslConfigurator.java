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

import java.security.KeyPair;
import java.security.PrivateKey;
import java.security.cert.Certificate;
import java.util.Base64;
import java.util.Properties;

/**
 * SAP CPI Keystore bridge for Kafka SSL/SASL_SSL.
 *
 * Supports:
 *   - SASL_SSL with private/self-signed broker CA via certificateAlias.
 *   - SSL/mTLS with client private key via mtlsKeyAlias.
 *   - PEM in-memory Kafka SSL properties; no filesystem truststore/keystore needed.
 */
public final class SdiaKafkaSslConfigurator {

    private static final String SSL_TRUSTSTORE_TYPE = "ssl.truststore.type";
    private static final String SSL_TRUSTSTORE_CERTIFICATES = "ssl.truststore.certificates";
    private static final String SSL_KEYSTORE_TYPE = "ssl.keystore.type";
    private static final String SSL_KEYSTORE_KEY = "ssl.keystore.key";
    private static final String SSL_KEYSTORE_CERTIFICATE_CHAIN = "ssl.keystore.certificate.chain";

    private SdiaKafkaSslConfigurator() { }

    public static void applySslMaterial(final SdiaKafkaEndpoint endpoint,
                                        final Properties props,
                                        final boolean tlsEnabled) {
        if (!tlsEnabled || endpoint == null || props == null) {
            return;
        }

        props.put("ssl.endpoint.identification.algorithm", "https");

        final String brokerCaAlias = trimToNull(endpoint.getCertificateAlias());
        final String brokerCaSource = trimToNull(endpoint.getBrokerCaSource());
        if (brokerCaAlias != null && brokerCaSource != null
                && brokerCaSource.toLowerCase(java.util.Locale.ROOT).indexOf("custom") >= 0) {
            applyBrokerCaCertificate(brokerCaAlias, props);
        }

        final String clientKeyAlias = trimToNull(endpoint.getMtlsKeyAlias());
        if (clientKeyAlias != null) {
            applyClientPrivateKey(clientKeyAlias, props);
        }
    }

    private static void applyBrokerCaCertificate(final String alias, final Properties props) {
        final KeystoreService keystore = getKeystoreService();
        final Certificate[] certificates = resolveCertificateChainOrSingle(keystore, alias);
        if (certificates == null || certificates.length == 0) {
            throw new IllegalStateException("Broker CA Certificate alias not found in SAP CPI Keystore: " + alias);
        }
        props.put(SSL_TRUSTSTORE_TYPE, "PEM");
        props.put(SSL_TRUSTSTORE_CERTIFICATES, certificatesToPem(certificates));
    }

    private static void applyClientPrivateKey(final String alias, final Properties props) {
        final KeystoreService keystore = getKeystoreService();
        try {
            final KeyPair keyPair = keystore.getKeyPair(alias);
            if (keyPair == null || keyPair.getPrivate() == null) {
                throw new IllegalStateException("mTLS key alias returned no private key: " + alias);
            }

            final Certificate[] chain = resolveCertificateChainOrSingle(keystore, alias);
            if (chain == null || chain.length == 0) {
                throw new IllegalStateException("mTLS key alias returned no certificate chain: " + alias);
            }

            props.put(SSL_KEYSTORE_TYPE, "PEM");
            props.put(SSL_KEYSTORE_KEY, privateKeyToPem(keyPair.getPrivate()));
            props.put(SSL_KEYSTORE_CERTIFICATE_CHAIN, certificatesToPem(chain));
        } catch (KeystoreException e) {
            throw new IllegalStateException("Could not read mTLS key alias from SAP CPI Keystore: " + alias, e);
        }
    }

    private static Certificate[] resolveCertificateChainOrSingle(final KeystoreService keystore, final String alias) {
        try {
            final Certificate[] chain = keystore.getCertificateChain(alias);
            if (chain != null && chain.length > 0) {
                return chain;
            }
        } catch (Throwable ignored) { }

        try {
            final Certificate certificate = keystore.getCertificate(alias);
            if (certificate != null) {
                return new Certificate[] { certificate };
            }
        } catch (KeystoreException e) {
            throw new IllegalStateException("Could not read certificate alias from SAP CPI Keystore: " + alias, e);
        }
        return new Certificate[0];
    }

    private static KeystoreService getKeystoreService() {
        try {
            final KeystoreService keystoreService = ITApiFactory.getApi(KeystoreService.class, null);
            if (keystoreService == null) {
                throw new IllegalStateException("SAP CPI KeystoreService is not available in this runtime.");
            }
            return keystoreService;
        } catch (InvalidContextException e) {
            throw new IllegalStateException("Could not access SAP CPI KeystoreService.", e);
        }
    }

    private static String certificatesToPem(final Certificate[] certificates) {
        final StringBuilder pem = new StringBuilder(certificates.length * 1600);
        for (Certificate certificate : certificates) {
            if (certificate == null) {
                continue;
            }
            try {
                pem.append("-----BEGIN CERTIFICATE-----\n");
                pem.append(Base64.getMimeEncoder(64, new byte[] {'\n'}).encodeToString(certificate.getEncoded()));
                pem.append("\n-----END CERTIFICATE-----\n");
            } catch (Exception e) {
                throw new IllegalStateException("Could not encode certificate to PEM.", e);
            }
        }
        return pem.toString();
    }

    private static String privateKeyToPem(final PrivateKey privateKey) {
        if (privateKey == null || privateKey.getEncoded() == null) {
            throw new IllegalStateException("Private key is empty or not exportable as PKCS#8.");
        }
        final StringBuilder pem = new StringBuilder(2048);
        pem.append("-----BEGIN PRIVATE KEY-----\n");
        pem.append(Base64.getMimeEncoder(64, new byte[] {'\n'}).encodeToString(privateKey.getEncoded()));
        pem.append("\n-----END PRIVATE KEY-----\n");
        return pem.toString();
    }

    private static String trimToNull(final String value) {
        if (value == null) {
            return null;
        }
        final String trimmed = value.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }
}
