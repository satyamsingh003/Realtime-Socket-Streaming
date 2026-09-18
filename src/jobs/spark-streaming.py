import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json,col
from pyspark.sql.types import (StructType,StructField,StringType,DoubleType,IntegerType)


def start_streaming(spark):
    stream_df=spark.readStream.format("socket").option("host","localhost").option("port",9999).load()

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

    parsed_df=(stream_df.select(from_json(col("value"),schema).alias("data")))

    parsed_df.printSchema()
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

    filtered_df=(parsed_df.filter(col("stars")>=4)
                 .select("review_id","stars","text"))

    # stream_df.printSchema()
    # query=stream_df.writeStream.outputMode("append").format("console").start()
    query = filtered_df.writeStream.outputMode("append").format("console").start()
    query.awaitTermination()




if __name__ == "__main__":
    SparkConn=SparkSession.builder.appName("SocketStreaming").getOrCreate()
    start_streaming(SparkConn)
