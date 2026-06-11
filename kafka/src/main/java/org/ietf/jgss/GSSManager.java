package org.ietf.jgss;

/**
 * CPI/OSGi stub for Kafka's optional native GSSAPI branch.
 * The branch is not used when sasl.mechanism=PLAIN.
 */
public class GSSManager {
    private static final GSSManager INSTANCE = new GSSManager();

    public static GSSManager getInstance() {
        return INSTANCE;
    }

    public GSSName createName(String name, Oid nameType) throws GSSException {
        return new StubGSSName(name, nameType);
    }

    public GSSCredential createCredential(GSSName name, int lifetime, Oid mechanism, int usage) throws GSSException {
        return new StubGSSCredential(name, lifetime, mechanism, usage);
    }

    private static final class StubGSSName implements GSSName {
        private final String name;
        private final Oid type;
        private StubGSSName(String name, Oid type) {
            this.name = name;
            this.type = type;
        }
        public String toString() { return name + "[" + type + "]"; }
    }

    private static final class StubGSSCredential implements GSSCredential {
        private final GSSName name;
        private final int lifetime;
        private final Oid mechanism;
        private final int usage;
        private StubGSSCredential(GSSName name, int lifetime, Oid mechanism, int usage) {
            this.name = name;
            this.lifetime = lifetime;
            this.mechanism = mechanism;
            this.usage = usage;
        }
        public String toString() {
            return String.valueOf(name) + ":" + lifetime + ":" + mechanism + ":" + usage;
        }
    }
}
