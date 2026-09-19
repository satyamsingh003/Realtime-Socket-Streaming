import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json,col
from pyspark.sql.types import (StructType,StructField,StringType,DoubleType,IntegerType)


def process_batch(batch_df,batch_id):
    print("\n")
    print("="*70)
    print(f"Micro Batch Id: {batch_id}")
    print("="*70)

    # Count records in hte micro batches
    records_count=batch_df.count()

    print(f"Number of records after filter: {records_count}")

    #Display records
    batch_df.select("review_id",
        "stars",
        "text"
    ).show(
        10,
        truncate=False
    )


def start_streaming(spark):
    stream_df=spark.readStream.format("socket").option("host","localhost").option("port",9999).load()

    #  Define the JSON schema

    schema=StructType([
        StructField("review_id",StringType(),True),
        StructField("user_id",StringType(),True),
        StructField("business_id",StringType(),True),
        StructField("stars",DoubleType(),True),
        StructField("useful",IntegerType(),True),
        StructField("funny",IntegerType(),True),
        StructField("cool",IntegerType(),True),
        StructField("text",StringType(),True),
        StructField("date",StringType(),True)
    ])

    #Convert JSON string -> STRUCT

    parsed_df=(stream_df.select(from_json(col("value"),schema).alias("data")))

    parsed_df.printSchema()

    # Extract fields from STRUCT

    parsed_df = parsed_df.select(
        col("data.review_id").alias("review_id"),
        col("data.user_id").alias("user_id"),
        col("data.business_id").alias("business_id"),
        col("data.stars").alias("stars"),
        col("data.useful").alias("useful"),
        col("data.funny").alias("funny"),
        col("data.cool").alias("cool"),
        col("data.text").alias("text"),
        col("data.date").alias("date")
    )

    #Filter reviews with 4 or more stars
    filtered_df=(parsed_df.filter(col("stars")>=4)
                 .select("review_id","stars","text"))


    # stream_df.printSchema()
    # query=stream_df.writeStream.outputMode("append").format("console").start()
    # query = filtered_df.writeStream.outputMode("append").format("console").start()

    # Write streaming output to console
    # query = (
    #     filtered_df
    #     .writeStream
    #     .outputMode("append")
    #     .format("console")
    #     .option("truncate", False)
    #     .option("numRows", 10)
    #     .trigger(processingTime="10 seconds")
    #     .start()
    # )

    query = (
        filtered_df
        .writeStream
        .outputMode("append")
        .foreachBatch(process_batch)
        .trigger(processingTime="10 seconds")
        .start()
    )
    query.awaitTermination()




if __name__ == "__main__":
    spark = (
        SparkSession
        .builder
        .appName("SocketStreaming")
        .getOrCreate()
    )

    # Reduce Spark's internal logs so our output is easier to read
    spark.sparkContext.setLogLevel("WARN")

    start_streaming(spark)
#     SparkConn=SparkSession.builder.appName("SocketStreaming").getOrCreate()
#     start_streaming(SparkConn)

