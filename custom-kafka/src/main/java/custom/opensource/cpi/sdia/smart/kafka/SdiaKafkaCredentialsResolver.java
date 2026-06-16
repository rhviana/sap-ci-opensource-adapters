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
import com.sap.it.api.securestore.SecureStoreService;
import com.sap.it.api.securestore.UserCredential;
import com.sap.it.api.securestore.exception.SecureStoreException;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Resolver for SAP CPI Security Material with per-alias caching.
 * Eliminates double cache lookups and ensures immediate heap garbage mitigation.
 */
public final class SdiaKafkaCredentialsResolver {

    // Inicialização com capacidade inicial explícita para cenários típicos de iFlow (poucos aliases)
    private static final Map<String, SdiaKafkaCredentials> CACHE = new ConcurrentHashMap<String, SdiaKafkaCredentials>(16, 0.75f, 2);
    private static final Map<String, Object> LOCKS = new ConcurrentHashMap<String, Object>(16, 0.75f, 2);

    private SdiaKafkaCredentialsResolver() {
    }

    public static SdiaKafkaCredentials resolve(final String credentialAlias) {
        final String alias = trimToNull(credentialAlias);
        if (alias == null) {
            throw new IllegalArgumentException(
                    "Missing Credential Name. Maintain a SAP CPI Security Material entry of type User Credentials.");
        }

        // Primeiro lookup rápido e thread-safe no cache
        SdiaKafkaCredentials cached = CACHE.get(alias);
        if (cached != null) {
            return cached;
        }

        // Bloco sincronizado por alias sem String.intern(), evitando pinagem no pool global de Strings do worker OSGi.
        final Object lock = lockFor(alias);
        synchronized (lock) {
            // Double-checked locking contra concorrência imediata pós-bloqueio
            cached = CACHE.get(alias);
            if (cached != null) {
                return cached;
            }

            char[] passwordChars = null;
            try {
                final SecureStoreService secureStoreService = ITApiFactory.getApi(SecureStoreService.class, null);
                if (secureStoreService == null) {
                    throw new IllegalStateException("SAP CPI SecureStoreService is not available in this runtime.");
                }

                final UserCredential userCredential = secureStoreService.getUserCredential(alias);
                if (userCredential == null) {
                    throw new IllegalStateException("Security Material alias returned no UserCredential: " + alias);
                }

                final String username = trimToNull(userCredential.getUsername());
                if (username == null) {
                    throw new IllegalStateException("Security Material alias has empty username: " + alias);
                }

                passwordChars = userCredential.getPassword();
                if (passwordChars == null || passwordChars.length == 0) {
                    throw new IllegalStateException("Security Material alias has empty password: " + alias);
                }

                // Criação da string de destino final de uma única vez
                final String password = new String(passwordChars);

                final SdiaKafkaCredentials resolved = new SdiaKafkaCredentials(username, password);
                CACHE.put(alias, resolved);
                return resolved;

            } catch (SecureStoreException e) {
                throw new IllegalStateException("Could not read SAP CPI Security Material alias: " + alias, e);
            } catch (InvalidContextException e) {
                throw new IllegalStateException("Could not access SAP CPI SecureStoreService for alias: " + alias, e);
            } finally {
                // Prática de Segurança e GC: Sobrescreve o array de caracteres temporário no Heap para evitar exposição e fragmentação
                if (passwordChars != null) {
                    Arrays.fill(passwordChars, ' ');
                }
            }
        }
    }

    public static void clearCache() {
        CACHE.clear();
    }

    private static Object lockFor(final String alias) {
        final Object created = new Object();
        final Object existing = LOCKS.putIfAbsent(alias, created);
        return existing == null ? created : existing;
    }

    private static String trimToNull(final String value) {
        if (value == null) {
            return null;
        }
        final String trimmed = value.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }
}