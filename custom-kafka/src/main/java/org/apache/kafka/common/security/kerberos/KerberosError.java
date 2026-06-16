package org.apache.kafka.common.security.kerberos;

/**
 * CPI/OSGi stub. This adapter build does not support Kerberos/GSSAPI.
 * Kafka's PLAIN/SASL_SSL path references this enum for error classification only.
 */
public enum KerberosError {
    SERVER_NOT_FOUND(false),
    CLIENT_NOT_YET_VALID(true),
    TICKET_NOT_YET_VALID(true),
    REPLAY(true);

    private final boolean retriable;

    KerberosError(boolean retriable) {
        this.retriable = retriable;
    }

    public boolean retriable() {
        return retriable;
    }

    public static KerberosError fromException(Exception exception) {
        return null;
    }

    public static boolean isRetriableClientGssException(Exception exception) {
        return false;
    }
}
