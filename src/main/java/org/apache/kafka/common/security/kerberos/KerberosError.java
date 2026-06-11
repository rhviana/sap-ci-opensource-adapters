/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.common.security.kerberos;

/**
 * CPI/OSGi-safe KerberosError replacement for EventSmartKafka v1.0.
 *
 * The upstream Kafka client class imports org.ietf.jgss.GSSException and also
 * probes JDK-internal Kerberos classes. SAP CPI OSGi tenants may not expose
 * those packages to custom adapter bundles, even when the active mechanism is
 * SCRAM or PLAIN rather than GSSAPI.
 *
 * EventSmartKafka v1.0 explicitly does not support Kerberos/GSSAPI. This class
 * keeps Kafka's non-Kerberos SASL path binary-compatible while removing the
 * hard GSS/JDK-internal class dependency from the adapter bundle.
 */
public enum KerberosError {
    SERVER_NOT_FOUND(7, false),
    CLIENT_NOT_YET_VALID(21, true),
    TICKET_NOT_YET_VALID(33, true),
    REPLAY(34, true);

    private final int errorCode;
    private final boolean retriable;

    KerberosError(final int errorCode, final boolean retriable) {
        this.errorCode = errorCode;
        this.retriable = retriable;
    }

    public boolean retriable() {
        return retriable;
    }

    public int errorCode() {
        return errorCode;
    }

    public static KerberosError fromException(final Exception exception) {
        return null;
    }

    public static boolean isRetriableClientGssException(final Exception exception) {
        return false;
    }
}
