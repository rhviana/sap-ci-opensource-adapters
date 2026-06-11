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

import com.sap.it.api.ITApiFactory;
import com.sap.it.api.exception.InvalidContextException;
import com.sap.it.api.securestore.SecureStoreService;
import com.sap.it.api.securestore.UserCredential;
import com.sap.it.api.securestore.exception.SecureStoreException;

import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Thread-safe resolver for SAP CPI Security Material (User Credentials).
 *
 * <h3>Caching</h3>
 * Resolved credentials are cached for the lifetime of the adapter deployment.
 * The cache is cleared on {@code doStart()} (adapter redeploy / iFlow restart),
 * ensuring that rotated credentials take effect without requiring a platform
 * restart.
 *
 * <h3>Thread safety</h3>
 * Uses {@link ConcurrentHashMap#computeIfAbsent} to guarantee that the
 * {@link SecureStoreService} is called at most once per alias under concurrent
 * access. This replaces the previous {@code synchronized(alias.intern())} pattern,
 * which was unsafe in an OSGi multi-classloader environment: {@code String.intern()}
 * stores strings in the JVM's shared string pool (PermGen / Metaspace), where they
 * can never be garbage-collected, and synchronizing on pool entries leaks lock
 * objects across OSGi bundle classloaders.
 *
 * <h3>Security</h3>
 * The password {@code char[]} returned by {@link UserCredential#getPassword()} is
 * zeroed immediately after the {@code String} is constructed, minimising the window
 * during which the cleartext password occupies heap memory.
 */
public final class SdiaKafkaCredentialsResolver {

    private static final ConcurrentHashMap<String, SdiaKafkaCredentials> CACHE =
            new ConcurrentHashMap<String, SdiaKafkaCredentials>(16, 0.75f, 2);

    private SdiaKafkaCredentialsResolver() {}

    /**
     * Resolves the {@link SdiaKafkaCredentials} for the given SAP CPI Security
     * Material alias. The result is cached; repeated calls for the same alias
     * return the cached instance without hitting the Secure Store.
     *
     * @param credentialAlias non-null, non-blank SAP CPI Security Material alias
     * @return resolved credentials
     * @throws IllegalArgumentException if the alias is blank
     * @throws IllegalStateException    if the Secure Store is unavailable or the
     *                                  alias does not exist / is empty
     */
    public static SdiaKafkaCredentials resolve(final String credentialAlias) {
        final String alias = trimToNull(credentialAlias);
        if (alias == null) {
            throw new IllegalArgumentException(
                    "Missing Credential Name. Maintain a SAP CPI Security Material entry of type User Credentials.");
        }

        // computeIfAbsent guarantees the loader runs at most once per alias,
        // even under concurrent calls, without synchronizing on a String.
        final SdiaKafkaCredentials result = CACHE.computeIfAbsent(alias, SdiaKafkaCredentialsResolver::load);
        if (result == null) {
            throw new IllegalStateException("Credential resolution returned null for alias: " + alias);
        }
        return result;
    }

    /** Clears the credential cache. Called on adapter redeploy via {@code doStart()}. */
    public static void clearCache() {
        CACHE.clear();
    }

    // -------------------------------------------------------------------------
    // Internal loader — called by computeIfAbsent, exactly once per alias
    // -------------------------------------------------------------------------

    private static SdiaKafkaCredentials load(final String alias) {
        char[] passwordChars = null;
        try {
            final SecureStoreService svc = ITApiFactory.getApi(SecureStoreService.class, null);
            if (svc == null) {
                throw new IllegalStateException("SAP CPI SecureStoreService is not available in this runtime.");
            }
            final UserCredential credential = svc.getUserCredential(alias);
            if (credential == null) {
                throw new IllegalStateException("Security Material alias returned no UserCredential: " + alias);
            }
            final String username = trimToNull(credential.getUsername());
            if (username == null) {
                throw new IllegalStateException("Security Material alias has empty username: " + alias);
            }
            passwordChars = credential.getPassword();
            if (passwordChars == null || passwordChars.length == 0) {
                throw new IllegalStateException("Security Material alias has empty password: " + alias);
            }
            final String password = new String(passwordChars);
            return new SdiaKafkaCredentials(username, password);

        } catch (final SecureStoreException e) {
            throw new IllegalStateException("Could not read SAP CPI Security Material alias: " + alias, e);
        } catch (final InvalidContextException e) {
            throw new IllegalStateException("Could not access SAP CPI SecureStoreService for alias: " + alias, e);
        } finally {
            // Zero the char[] immediately — minimises the window the cleartext password
            // occupies heap memory, reducing exposure in heap dumps.
            if (passwordChars != null) Arrays.fill(passwordChars, ' ');
        }
    }

    private static String trimToNull(final String value) {
        if (value == null) return null;
        final String t = value.trim();
        return t.isEmpty() ? null : t;
    }
}
