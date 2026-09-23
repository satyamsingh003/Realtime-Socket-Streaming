from ensurepip import bootstrap
from importlib.metadata import metadata

from kafka import KafkaProducer
import time
import json
import uuid

ACKS='all'



producer= KafkaProducer(
    bootstrap_servers=[
        "localhost:19092",
        "localhost:29092",
        "localhost:39092",
    ],
    acks=ACKS,
    retries=3,
    retry_backoff_ms=500,
    request_timeout_ms=3000,
    delivery_timeout_ms=10000,

)


print("=" * 60)
print("KAFKA PRODUCER RELIABILITY LAB")
print("=" * 60)

print(f"acks                : {ACKS}")
print("retries             : 3")
print("retry_backoff_ms    : 500")
print("request_timeout_ms  : 3000")
print("delivery_timeout_ms : 10000")
print("=" * 60)

for i  in range(100):
    value= f"retry-test-record-{i}".encode("utf-8")

    start = time.perf_counter()

    try:
        metadata=producer.send(
            "producer-retry-lab",
            key=f"key-{i}".encode("utf-8"),
            value=value
        ).get(timeout=20)

        elapsed=time.perf_counter()-start

        print(
        f"SUCCESS | "
        f"record={i} | "
        f"partition={metadata.partition} | "
        f"offset={metadata.offset} | "
        f"time={elapsed:.3f}s")


    except Exception as e:

        elapsed = time.perf_counter() - start

        print(
            f"FAILED  | "
            f"record={i} | "
            f"time={elapsed:.3f}s | "
            f"error={type(e).__name__}: {e}"
        )
    time.sleep(0.2)

producer.flush()
producer.close()

print("\nProducer finished.")
