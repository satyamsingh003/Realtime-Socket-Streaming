from kafka import KafkaProducer
import time
import uuid

# COMPRESSION = None   # Change to "lz4", "zstd", or "gzip"
# COMPRESSION = "lz4"
# COMPRESSION = "zstd"
COMPRESSION = "gzip"

producer_config = {
    "bootstrap_servers": "localhost:19092",
    # "batch_size": 16384,
    "batch_size":65536,
    "linger_ms": 100,
    "acks": "all",
}

if COMPRESSION:
    producer_config["compression_type"] = COMPRESSION

producer = KafkaProducer(**producer_config)

experiment_id = str(uuid.uuid4())[:8]

print("\n==============================")
print("KAFKA COMPRESSION LAB")
print("==============================")
print(f"Experiment   : {experiment_id}")
print(f"Compression  : {COMPRESSION or 'none'}")
print("Records      : 10000")
print("Batch size   : 65536")
print("Linger       : 100 ms")
print("Acks         : all")
print("==============================\n")

start = time.perf_counter()

futures = []

for i in range(10000):
    value = (
        f"compression-{experiment_id}-"
        f"record-{i}-"
        f"customer-data-north-region-product-sales-"
        f"repeated-business-data"
    ).encode("utf-8")

    future = producer.send(
        "key-partition-lab",
        value=value
    )

    futures.append(future)

# Explicitly wait for delivery
successful = 0
failed = 0

for future in futures:
    try:
        future.get(timeout=30)
        successful += 1
    except Exception as e:
        failed += 1
        print("Delivery failed:", e)

producer.flush()

elapsed = time.perf_counter() - start

print("\n==============================")
print("RESULT")
print("==============================")
print(f"Successful  : {successful}")
print(f"Failed      : {failed}")
print(f"Elapsed     : {elapsed:.3f} sec")
print(f"Throughput  : {successful / elapsed:.2f} records/sec")
print("==============================")

producer.close()