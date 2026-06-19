#!/usr/bin/env python3
"""
EventSmartKafka — Smoke Validation x10

Sends 10 messages per topic across the full 20-topic matrix.
Use this before the 800k stress run.

Run from:
    kafka-local-demo\tests\smoke or tests\stress

Requires:
    pip install confluent-kafka
"""

import os
import sys
import time
from typing import Dict, Optional, Tuple
from dataclasses import dataclass
from confluent_kafka import Producer


USER = "sdia"
PWD  = "SDIABRASIL"
CERT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'security', 'certificates', 'ca.crt'))
HOST = "127.0.0.1"

MSGS_PER_TOPIC = 10
BINS = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'payloads', 'avro', 'binary'))

F_53B  = "payload-magic-ordersimple-53b.bin"
F_1MB  = "avro-magic-1mb.bin"
F_2MB  = "avro-nomagic-shipment-1999kb.bin"

PLAINTEXT      = ("PLAINTEXT",      None)
SASL_PLAIN     = ("SASL_PLAINTEXT", "PLAIN")
SASL_P_256     = ("SASL_PLAINTEXT", "SCRAM-SHA-256")
SASL_P_512     = ("SASL_PLAINTEXT", "SCRAM-SHA-512")
SASL_SSL_PLAIN = ("SASL_SSL",       "PLAIN")
SASL_SSL_256   = ("SASL_SSL",       "SCRAM-SHA-256")
SASL_SSL_512   = ("SASL_SSL",       "SCRAM-SHA-512")


@dataclass(frozen=True)
class TopicSpec:
    topic: str
    payload_file: str
    port: int
    protocol: str
    mechanism: Optional[str]


TESTS = [
    TopicSpec("id00.acme.sales.otc.orders.created.n.s",               F_53B, 19101, *PLAINTEXT),
    TopicSpec("id01.acme.sales.otc.fulfillment.started.s.p",          F_53B, 19102, *SASL_PLAIN),
    TopicSpec("id02.acme.sales.otc.fulfillment.started.s.tls.p",      F_53B, 19103, *SASL_SSL_PLAIN),
    TopicSpec("id03.acme.sales.otc.orders.updated.s.s.256",           F_53B, 19102, *SASL_P_256),
    TopicSpec("id04.acme.sales.otc.orders.updated.s.tls.s.256",       F_53B, 19103, *SASL_SSL_256),
    TopicSpec("id05.acme.sales.otc.shipments.dispatched.s.s.512",     F_53B, 19102, *SASL_P_512),
    TopicSpec("id06.acme.sales.otc.shipments.dispatched.s.tls.s.512", F_53B, 19103, *SASL_SSL_512),

    TopicSpec("id13.acme.sales.otc.orders.created.n.s.cj",            F_53B, 19101, *PLAINTEXT),
    TopicSpec("id14.acme.sales.otc.orders.created.n.s.cj.1mb",        F_1MB, 19101, *PLAINTEXT),
    TopicSpec("id15.acme.sales.otc.orders.created.n.s.cj.2mb",        F_2MB, 19101, *PLAINTEXT),
    TopicSpec("id16.acme.sales.otc.orders.created.n.s.cx",            F_53B, 19101, *PLAINTEXT),
    TopicSpec("id17.acme.sales.otc.orders.created.n.s.cx.1mb",        F_1MB, 19101, *PLAINTEXT),
    TopicSpec("id18.acme.sales.otc.orders.created.n.s.cx.2mb",        F_2MB, 19101, *PLAINTEXT),

    TopicSpec("id19.acme.sales.otc.orders.created.n.s.cj.53kb",       F_53B, 19102, *SASL_PLAIN),
    TopicSpec("id20.acme.sales.otc.orders.created.n.s.cj.53kb",       F_53B, 19102, *SASL_PLAIN),
    TopicSpec("id21.acme.sales.otc.orders.created.n.s.cj.1mb",        F_1MB, 19102, *SASL_P_256),
    TopicSpec("id22.acme.sales.otc.orders.created.n.s.cj.2mb",        F_2MB, 19102, *SASL_P_512),

    TopicSpec("id23.acme.sales.otc.orders.created.n.s.cj.53kb",       F_53B, 19103, *SASL_SSL_PLAIN),
    TopicSpec("id24.acme.sales.otc.orders.created.n.s.cj.1mb",        F_1MB, 19103, *SASL_SSL_256),
    TopicSpec("id25.acme.sales.otc.orders.created.n.s.cj.2mb",        F_2MB, 19103, *SASL_SSL_512),
]


def load_payload(fname: str) -> bytes:
    path = os.path.join(BINS, fname)
    with open(path, "rb") as f:
        return f.read()


def make_producer(port: int, protocol: str, mechanism: Optional[str]) -> Producer:
    cfg = {
        "bootstrap.servers": f"{HOST}:{port}",
        "acks": "1",
        "message.max.bytes": 5_000_000,
        "socket.timeout.ms": 30_000,
        "request.timeout.ms": 30_000,
        "message.timeout.ms": 120_000,
        "linger.ms": 5,
        "batch.num.messages": 1000,
        "queue.buffering.max.messages": 100_000,
        "queue.buffering.max.kbytes": 524_288,
        "compression.type": "none",
    }

    if protocol == "PLAINTEXT":
        cfg["security.protocol"] = "PLAINTEXT"
    elif protocol == "SASL_PLAINTEXT":
        cfg.update({
            "security.protocol": "SASL_PLAINTEXT",
            "sasl.mechanism": mechanism,
            "sasl.username": USER,
            "sasl.password": PWD,
        })
    elif protocol == "SASL_SSL":
        cfg.update({
            "security.protocol": "SASL_SSL",
            "sasl.mechanism": mechanism,
            "sasl.username": USER,
            "sasl.password": PWD,
            "ssl.ca.location": CERT,
            "ssl.endpoint.identification.algorithm": "none",
        })
    else:
        raise ValueError(f"Unsupported protocol: {protocol}")

    return Producer(cfg)


def fmt_duration(seconds: float) -> str:
    seconds = max(0, int(seconds))
    h, rem = divmod(seconds, 3600)
    m, s = divmod(rem, 60)
    return f"{h:02d}:{m:02d}:{s:02d}"


def main() -> None:
    if not os.path.isfile(CERT):
        print(f"ERROR: CA cert not found: {CERT}")
        sys.exit(1)

    payloads: Dict[str, bytes] = {}
    for fname in (F_53B, F_1MB, F_2MB):
        path = os.path.join(BINS, fname)
        if not os.path.isfile(path):
            print(f"MISSING PAYLOAD: {path}")
            sys.exit(1)
        payloads[fname] = load_payload(fname)

    total_target = len(TESTS) * MSGS_PER_TOPIC
    total_bytes = sum(MSGS_PER_TOPIC * len(payloads[t.payload_file]) for t in TESTS)

    print("EventSmartKafka — Smoke Validation x10\n")
    print(f"Topics       : {len(TESTS)}")
    print(f"Per topic    : {MSGS_PER_TOPIC}")
    print(f"Total msgs   : {total_target:,}")
    print(f"Total bytes  : {total_bytes:,} B | {total_bytes / (1024**2):.2f} MiB\n")

    producers: Dict[Tuple[int, str, Optional[str]], Producer] = {}
    for spec in TESTS:
        key = (spec.port, spec.protocol, spec.mechanism)
        if key not in producers:
            producers[key] = make_producer(*key)

    ok = 0
    fail = 0
    bytes_ok = 0
    per_topic = {spec.topic: {"ok": 0, "fail": 0, "err": None} for spec in TESTS}

    def make_cb(topic: str, payload_len: int):
        def cb(err, msg):
            nonlocal ok, fail, bytes_ok
            if err:
                fail += 1
                per_topic[topic]["fail"] += 1
                per_topic[topic]["err"] = str(err)
            else:
                ok += 1
                bytes_ok += payload_len
                per_topic[topic]["ok"] += 1
        return cb

    start = time.monotonic()

    for spec in TESTS:
        payload = payloads[spec.payload_file]
        p = producers[(spec.port, spec.protocol, spec.mechanism)]
        for _ in range(MSGS_PER_TOPIC):
            p.produce(spec.topic, value=payload, callback=make_cb(spec.topic, len(payload)))
            p.poll(0)

        label = f"{spec.protocol}/{spec.mechanism or '-'}"
        print(f"  SENT  :{spec.port} {label:28} {spec.topic:62} {MSGS_PER_TOPIC}x ({len(payload):,} B)")

    print("\nFlushing...")
    for p in producers.values():
        p.flush(120)

    elapsed = max(0.001, time.monotonic() - start)

    print("\nPer-topic result")
    for spec in TESTS:
        r = per_topic[spec.topic]
        label = f"{spec.protocol}/{spec.mechanism or '-'}"
        status = "OK" if r["fail"] == 0 and r["ok"] == MSGS_PER_TOPIC else "FAIL"
        print(f"  {status:5} :{spec.port} {label:28} {spec.topic:62} ok={r['ok']:3} fail={r['fail']:3}")
        if r["err"]:
            print(f"        last error: {r['err']}")

    print()
    print(f"Total elapsed : {fmt_duration(elapsed)}")
    print(f"Total OK      : {ok:,}")
    print(f"Total Failed  : {fail:,}")
    print(f"Avg msg/s     : {ok / elapsed:,.0f}")
    print(f"Avg MiB/s     : {bytes_ok / elapsed / (1024**2):,.2f}")

    if fail:
        sys.exit(1)


if __name__ == "__main__":
    main()
