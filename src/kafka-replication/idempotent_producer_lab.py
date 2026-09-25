from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers=[
        "localhost:19092",
        "localhost:29092",
        "localhost:39092",
    ],

    acks="all",

    retries=5,

    enable_idempotence=True,
)

print("=" * 60)
print("IDEMPOTENT PRODUCER LAB")
print("=" * 60)

print("acks              :", "all")
print("retries           :", 5)
print("enable_idempotence:", True)

print("=" * 60)

for i in range(20):

    value = f"idempotence-test-{i}".encode("utf-8")

    try:

        metadata = producer.send(
            "idempotence-lab",
            key=f"key-{i}".encode("utf-8"),
            value=value,
        ).get(timeout=20)

        print(
            f"SUCCESS | "
            f"record={i} | "
            f"partition={metadata.partition} | "
            f"offset={metadata.offset}"
        )

    except Exception as e:

        print(
            f"FAILED | "
            f"record={i} | "
            f"error={type(e).__name__}: {e}"
        )

producer.flush()
producer.close()

print("\nProducer finished.")


