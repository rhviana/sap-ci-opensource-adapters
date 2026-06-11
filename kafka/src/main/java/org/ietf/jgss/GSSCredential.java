package org.ietf.jgss;

/** CPI/OSGi stub for Kafka optional Kerberos/GSSAPI code path. */
public interface GSSCredential {
    int INITIATE_AND_ACCEPT = 0;
    int INITIATE_ONLY = 1;
    int ACCEPT_ONLY = 2;
    int DEFAULT_LIFETIME = 0;
    int INDEFINITE_LIFETIME = Integer.MAX_VALUE;
}
