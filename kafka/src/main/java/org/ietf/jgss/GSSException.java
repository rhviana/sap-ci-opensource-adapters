package org.ietf.jgss;

/**
 * CPI/OSGi runtime stub for Kafka's optional Kerberos/GSSAPI references.
 * This adapter supports SASL/PLAIN over TLS, not Kerberos/GSSAPI.
 */
public class GSSException extends Exception {
    private static final long serialVersionUID = 1L;
    private final int major;
    private final int minor;
    private final String minorMessage;

    public GSSException(int major) {
        super("GSSAPI is not supported by this CPI Kafka adapter build");
        this.major = major;
        this.minor = 0;
        this.minorMessage = null;
    }

    public GSSException(int major, int minor, String minorMessage) {
        super(minorMessage == null ? "GSSAPI is not supported by this CPI Kafka adapter build" : minorMessage);
        this.major = major;
        this.minor = minor;
        this.minorMessage = minorMessage;
    }

    public int getMajor() { return major; }
    public int getMinor() { return minor; }
    public String getMinorString() { return minorMessage; }
    public String getMajorString() { return getMessage(); }
}
