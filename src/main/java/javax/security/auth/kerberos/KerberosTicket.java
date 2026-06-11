package javax.security.auth.kerberos;

import javax.crypto.SecretKey;
import javax.security.auth.DestroyFailedException;
import javax.security.auth.Destroyable;
import java.io.Serializable;
import java.net.InetAddress;
import java.util.Date;

/** CPI/OSGi runtime stub for Kafka optional Kerberos/GSSAPI references. */
public final class KerberosTicket implements Destroyable, Serializable {
    private static final long serialVersionUID = 1L;
    private boolean destroyed;

    public KerberosTicket(byte[] asn1Encoding,
                          KerberosPrincipal client,
                          KerberosPrincipal server,
                          byte[] sessionKey,
                          int keyType,
                          boolean[] flags,
                          Date authTime,
                          Date startTime,
                          Date endTime,
                          Date renewTill,
                          InetAddress[] clientAddresses) {
    }

    public KerberosPrincipal getClient() { return null; }
    public KerberosPrincipal getServer() { return null; }
    public SecretKey getSessionKey() { return null; }
    public int getSessionKeyType() { return 0; }
    public boolean isForwardable() { return false; }
    public boolean isForwarded() { return false; }
    public boolean isProxiable() { return false; }
    public boolean isProxy() { return false; }
    public boolean isPostdated() { return false; }
    public boolean isRenewable() { return false; }
    public boolean isInitial() { return false; }
    public boolean[] getFlags() { return null; }
    public Date getAuthTime() { return null; }
    public Date getStartTime() { return null; }
    public Date getEndTime() { return null; }
    public Date getRenewTill() { return null; }
    public InetAddress[] getClientAddresses() { return null; }
    public byte[] getEncoded() { return new byte[0]; }
    public boolean isCurrent() { return false; }
    public void refresh() { throw new UnsupportedOperationException("Kerberos/GSSAPI is not supported by this adapter build"); }
    public void destroy() throws DestroyFailedException { destroyed = true; }
    public boolean isDestroyed() { return destroyed; }
    public String toString() { return "KerberosTicket stub - unsupported"; }
}
