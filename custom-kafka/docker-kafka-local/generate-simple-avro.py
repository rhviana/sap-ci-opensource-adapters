#!/usr/bin/env python3
"""
EventSmartKafka — Single-Message Validation
"""
from __future__ import annotations  # garante que 'str | None' funcione mesmo em Python < 3.10

import os, sys, time, gc
from confluent_kafka import Producer

# ── Credentials / cert ────────────────────────────────────────────────────────
USER = "sdia"
PWD  = "SDIABRASIL"
CERT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'certs', 'ca.crt'))
HOST = "127.0.0.1"
MSGS = 1

# Payload files live in the same folder as this script
BINS = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'payloads', 'avro'))

# ── Payload files ─────────────────────────────────────────────────────────────
F_53B  = "payload-magic-ordersimple-53b.bin"
F_1MB  = "avro-magic-1mb.bin"
F_2MB  = "avro-nomagic-shipment-1999kb.bin"

# ── Security tuples ─────────────────────────────────────────────────────────
PLAINTEXT      = ("PLAINTEXT",      None)
SASL_PLAIN     = ("SASL_PLAINTEXT", "PLAIN")
SASL_P_256     = ("SASL_PLAINTEXT", "SCRAM-SHA-256")
SASL_P_512     = ("SASL_PLAINTEXT", "SCRAM-SHA-512")
SASL_SSL_PLAIN = ("SASL_SSL",       "PLAIN")
SASL_SSL_256   = ("SASL_SSL",       "SCRAM-SHA-256")
SASL_SSL_512   = ("SASL_SSL",       "SCRAM-SHA-512")

# ── Test matrix ───────────────────────────────────────────────────────────────
TESTS = [
    ("id00.acme.sales.otc.orders.created.n.s", F_53B, 19101, PLAINTEXT),
    ("id01.acme.sales.otc.fulfillment.started.s.p", F_53B, 19102, SASL_PLAIN),
    ("id02.acme.sales.otc.fulfillment.started.s.tls.p", F_53B, 19103, SASL_SSL_PLAIN),
    ("id03.acme.sales.otc.orders.updated.s.s.256", F_53B, 19102, SASL_P_256),
    ("id04.acme.sales.otc.orders.updated.s.tls.s.256", F_53B, 19103, SASL_SSL_256),
    ("id05.acme.sales.otc.shipments.dispatched.s.s.512", F_53B, 19102, SASL_P_512),
    ("id06.acme.sales.otc.shipments.dispatched.s.tls.s.512", F_53B, 19103, SASL_SSL_512),
    ("id13.acme.sales.otc.orders.created.n.s.cj", F_53B, 19101, PLAINTEXT),
    ("id14.acme.sales.otc.orders.created.n.s.cj.1mb", F_1MB, 19101, PLAINTEXT),
    ("id15.acme.sales.otc.orders.created.n.s.cj.2mb", F_2MB, 19101, PLAINTEXT),
    ("id16.acme.sales.otc.orders.created.n.s.cx", F_53B, 19101, PLAINTEXT),
    ("id17.acme.sales.otc.orders.created.n.s.cx.1mb", F_1MB, 19101, PLAINTEXT),
    ("id18.acme.sales.otc.orders.created.n.s.cx.2mb", F_2MB, 19101, PLAINTEXT),
    ("id19.acme.sales.otc.orders.created.n.s.cj.53kb",F_53B, 19102, SASL_PLAIN),
    ("id20.acme.sales.otc.orders.created.n.s.cj.53kb",F_53B, 19102, SASL_PLAIN),
    ("id21.acme.sales.otc.orders.created.n.s.cj.1mb", F_1MB, 19102, SASL_P_256),
    ("id22.acme.sales.otc.orders.created.n.s.cj.2mb", F_2MB, 19102, SASL_P_512),
    ("id23.acme.sales.otc.orders.created.n.s.cj.53kb",F_53B, 19103, SASL_SSL_PLAIN),
    ("id24.acme.sales.otc.orders.created.n.s.cj.1mb", F_1MB, 19103, SASL_SSL_256),
    ("id25.acme.sales.otc.orders.created.n.s.cj.2mb", F_2MB, 19103, SASL_SSL_512),
]

def load_payload(fname: str) -> bytes:
    path = os.path.join(BINS, fname)
    with open(path, "rb") as f:
        return f.read()

def make_producer(port: int, protocol: str, mechanism: str | None) -> Producer:
    cfg = {
        "bootstrap.servers": f"{HOST}:{port}",
        "acks": "1",
        "message.max.bytes": 5_000_000,
        "socket.timeout.ms": 10_000,
    }
    if protocol == "PLAINTEXT":
        cfg["security.protocol"] = "PLAINTEXT"
    elif protocol.startswith("SASL"):
        cfg.update({
            "security.protocol": protocol,
            "sasl.mechanism": mechanism,
            "sasl.username": USER,
            "sasl.password": PWD,
        })
        if protocol == "SASL_SSL":
            cfg.update({
                "ssl.ca.location": CERT,
                "ssl.endpoint.identification.algorithm": "none",
            })
    return Producer(cfg)

def main():
    if not os.path.isfile(CERT):
        print(f"ERROR: CA cert not found: {CERT}", flush=True)
        sys.exit(1)

    cache = {fname: load_payload(fname) for fname in (F_53B, F_1MB, F_2MB)}

    print("EventSmartKafka — Executando testes...\n", flush=True)
    total_ok = 0
    total_fail = 0

    for topic, fname, port, (proto, mech) in TESTS:
        payload = cache[fname]
        counters = {"ok": 0, "fail": 0, "err": None}

        def delivery_cb(err, msg, c=counters):
            if err:
                c["fail"] += 1
                c["err"] = err
            else:
                c["ok"] += 1

        p = None
        try:
            p = make_producer(port, proto, mech)
            for _ in range(MSGS):
                p.produce(topic, value=payload, callback=delivery_cb)
                p.poll(0.1)  # Processa callbacks

            p.flush(10)

            label = f"{proto}/{mech or '-'}"
            if counters["fail"]:
                print(f"FAIL :{port} {label:28} {topic} -> {counters['err']}", flush=True)
                total_fail += 1
            else:
                print(f"OK   :{port} {label:28} {topic} ({len(payload):,} B)", flush=True)
                total_ok += 1
        except Exception as exc:
            print(f"ERROR:{port} {topic} -> {exc}", flush=True)
            total_fail += 1
        finally:
            # Libera explicitamente o producer (e suas threads nativas do librdkafka)
            # antes de criar o proximo, em vez de depender do GC pra fazer isso a tempo.
            if p is not None:
                del p
            gc.collect()

        time.sleep(0.2)

    print(f"\nResultados: {total_ok} OK, {total_fail} Falhas.", flush=True)
    if total_fail > 0:
        sys.exit(1)

if __name__ == "__main__":
    main()