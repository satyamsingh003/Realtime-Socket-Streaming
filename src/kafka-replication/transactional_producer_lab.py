from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers=[
        "localhost:19092",
        "localhost:29092",
        "localhost:39092",
    ],
    acks="all",
    enable_idempotence=True,
    transactional_id="transaction-producer-1",
)

print("=" * 60)
print("KAFKA TRANSACTION ABORT LAB")
print("=" * 60)

producer.init_transactions()

print("Transaction initialized.")

producer.begin_transaction()

print("Transaction started.")

events = [
    ("order-created", "customer-202"),
    ("payment-completed", "customer-202"),
    ("order-confirmed", "customer-202"),
]

for value, key in events:

    future = producer.send(
        "kafka-transactions-lab",
        key=key.encode(),
        value=value.encode(),
    )

    metadata = future.get(timeout=10)

    print(
        f"SENT | "
        f"key={key} | "
        f"value={value} | "
        f"partition={metadata.partition} | "
        f"offset={metadata.offset}"
    )

print("All 3 records acknowledged by Kafka.")

producer.abort_transaction()

print("Transaction ABORTED.")

producer.close()