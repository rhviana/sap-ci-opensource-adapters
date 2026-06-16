package org.ietf.jgss;

import java.io.Serializable;

/** CPI/OSGi stub for Kafka optional Kerberos/GSSAPI code path. */
public class Oid implements Serializable {
    private static final long serialVersionUID = 1L;
    private final String value;

    public Oid(String value) throws GSSException {
        if (value == null || value.length() == 0) {
            throw new GSSException(0, 0, "OID must not be empty");
        }
        this.value = value;
    }

    public String toString() { return value; }
}
