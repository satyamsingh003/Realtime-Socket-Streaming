from kafka import KafkaConsumer
import time

consumer = KafkaConsumer(
    "yelp-review-events",
    bootstrap_servers="localhost:9092",
    group_id="python-delivery-lab",
    enable_auto_commit=False,
    auto_offset_reset="latest",
    key_deserializer=lambda x: x.decode("utf-8") if x else None,
    value_deserializer=lambda x: x.decode("utf-8"),
)

print("Consumer started...")

message_count=0

for message in consumer:

    message_count+=1
    print("\n" + "=" * 60)
    print("RECEIVED MESSAGE")
    print("=" * 60)

    print(f"Topic     : {message.topic}")
    print(f"Partition : {message.partition}")
    print(f"Offset    : {message.offset}")
    print(f"Key       : {message.key}")
    print(f"Value     : {message.value}")

    print("\nPROCESSING MESSAGE...")
    time.sleep(2)

    if message_count == 1:
        print("COMMITTING FIRST MESSAGE...")
        consumer.commit()
        print("FIRST MESSAGE COMMITTED")

    else:
        print("💥 SIMULATED CRASH BEFORE OFFSET COMMIT")
        raise RuntimeError(
            "Application crashed before committing second message"
        )