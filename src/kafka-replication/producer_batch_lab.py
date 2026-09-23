from ensurepip import bootstrap

from kafka import KafkaProducer
import time

producer=KafkaProducer(
    bootstrap_servers="localhost:19092",
    linger_ms=100,
    batch_size=16384,
    acks="all",
)

for i in range(10000):
    producer.send(
        "key-partition-lab",
        value=f"batch-lab-{i}".encode("utf-8")

    )

producer.flush()

metrics=producer.metrics()

print(("\n===PRODUCER METRICS===="))

for metric_name,metric in metrics.items():
    print("\nNAME:", metric_name)
    print("TYPE:", type(metric))
    print("VALUE:", metric)
    name=metric_name

    if name in {
        "batch-size-avg",
        "record-send-rate",
        "record-size-avg",
        "request-rate",
        "record-queue-time-avg",
    }:
        print(f"{name}: {metric.value()}")
producer.close()