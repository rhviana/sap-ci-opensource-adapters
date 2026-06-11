package javax.security.auth.kerberos;

import java.io.Serializable;
import java.security.Principal;

/** CPI/OSGi runtime stub for Kafka optional Kerberos/GSSAPI references. */
public final class KerberosPrincipal implements Principal, Serializable {
    private static final long serialVersionUID = 1L;
    public static final int KRB_NT_UNKNOWN = 0;
    public static final int KRB_NT_PRINCIPAL = 1;
    public static final int KRB_NT_SRV_INST = 2;
    public static final int KRB_NT_SRV_HST = 3;
    public static final int KRB_NT_UID = 5;

    private final String name;
    private final int nameType;

    public KerberosPrincipal(String name) {
        this(name, KRB_NT_PRINCIPAL);
    }

    public KerberosPrincipal(String name, int nameType) {
        this.name = name == null ? "" : name;
        this.nameType = nameType;
    }

    public String getName() { return name; }
    public int getNameType() { return nameType; }
    public String getRealm() { return ""; }
    public int hashCode() { return name.hashCode(); }
    public boolean equals(Object other) {
        return other instanceof KerberosPrincipal && name.equals(((KerberosPrincipal) other).name);
    }
    public String toString() { return name; }
}
