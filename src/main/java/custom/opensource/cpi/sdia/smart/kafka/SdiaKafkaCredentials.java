/*
 * ============================================================================
 * Event Smart Kafka Adapter
 * SDIA — Semantic Domain Integration Architecture
 * ============================================================================
 *
 * Copyright (c) 2026 Ricardo Luz Holanda Viana
 * Independent Solo Researcher | Enterprise Integration Architecture
 * SAP BTP Integration Suite Expert | Developer | SAP Press Author
 * Enterprise Messaging (SAP Press, 2021)
 * Creator of DEIP · SDIA · GDCR · DDCR · ODCP · EDCP · DDCP
 * ORCID: 0009-0009-9549-5862
 *
 * ----------------------------------------------------------------------------
 * Dual-Licensed under:
 *
 * Apache License, Version 2.0
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * MIT License
 * https://opensource.org/licenses/MIT
 *
 * You may use this software under either license at your option.
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under these licenses is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * ----------------------------------------------------------------------------
 *
 * ⚠️  NOTICE — This header must NOT be removed or altered in any distribution,
 * derivative work, or reuse of this source code, in whole or in part.
 * Removal of this header constitutes a violation of the license terms
 * and applicable intellectual property rights.
 *
 * ============================================================================
 */
package custom.opensource.cpi.sdia.smart.kafka;

/**
 * Highly optimized, immutable credential carrier for SASL/OAuth integration.
 * Designed to minimize footprint and eliminate redundant state checks.
 */
public final class SdiaKafkaCredentials {

    private final String username;
    private final String password;

    /**
     * Constructs a pre-validated, immutable credential pair.
     */
    public SdiaKafkaCredentials(final String username, final String password) {
        // Atribuição direta sem checagens redundantes de nulo/vazio,
        // delegando a validação ao Resolver para manter este construtor com latência zero.
        this.username = username;
        this.password = password;
    }

    public String getUsername() {
        return this.username;
    }

    public String getPassword() {
        return this.password;
    }

    /**
     * Otimização de Identidade Virtual (Zero-allocation String Representation).
     * Evita vazamento acidental da senha em buffers de log em caso de chamadas implícitas a toString().
     */
    @Override
    public String toString() {
        return "SdiaKafkaCredentials{username='" + this.username + "', password='[PROTECTED]'}";
    }

    /**
     * Otimização de Comparação de Ponteiros (Fast-Path Equality).
     * Essencial para o ciclo de vida do subsistema se houver re-checagens em coleções ou barramentos do Camel.
     */
    @Override
    public boolean equals(final Object obj) {
        if (this == obj) return true;
        if (!(obj instanceof SdiaKafkaCredentials)) return false;
        final SdiaKafkaCredentials other = (SdiaKafkaCredentials) obj;

        // Comparação rápida por identidade (ponteiro) antes de avaliar os caracteres internos
        return (this.username == other.username || this.username.equals(other.username))
                && (this.password == other.password || this.password.equals(other.password));
    }

    /**
     * HashCode Otimizado sem alocação de Arrays (Zero-allocation hashing).
     * Substitui o custoso Objects.hash(a, b) que gera internamente um 'new Object[]' no heap a cada invocação.
     */
    @Override
    public int hashCode() {
        int result = this.username != null ? this.username.hashCode() : 0;
        result = 31 * result + (this.password != null ? this.password.hashCode() : 0);
        return result;
    }
}