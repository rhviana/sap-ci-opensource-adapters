package org.ietf.jgss;

/** CPI/OSGi stub for Kafka optional Kerberos/GSSAPI code path. */
public interface GSSName {
    Oid NT_HOSTBASED_SERVICE = createHostBasedOid();

    static Oid createHostBasedOid() {
        try {
            return new Oid("1.2.840.113554.1.2.1.4");
        } catch (GSSException e) {
            throw new IllegalStateException(e);
        }
    }
}
