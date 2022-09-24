package org.example

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, column, count, expr, from_json, max, schema_of_json, sum}
import org.apache.spark.sql.internal.connector.SimpleTableProvider
import org.apache.spark.streaming.{Seconds, StreamingContext}

//E:\Kaggle\input\Instagram

object Main {
  def main(args: Array[String]): Unit = {

    val brokers: String = "KAFKASERVER:9092,KAFKASERVER:9093,KAFKASERVER:9094,KAFKASERVER:9095"

    val spark = SparkSession
      .builder
      .appName("InstagramWithSparkStreaming")
      .config("spark.master", "local")
      .getOrCreate()

    val sc =spark.sparkContext
    sc.setLogLevel("WARN")

    import spark.implicits._

    val InstagramDf = spark
      .readStream.format("kafka")
      .option("kafka.bootstrap.servers", brokers)
      .option("subscribe", "instagram-locations")
      .option("startingOffsets", "latest")
      .option("failOnDataLoss", "false")
      .load()

    InstagramDf.printSchema()

//    val InstagramStringDF = InstagramDf.selectExpr("CAST(value AS STRING)")

    import org.apache.spark.sql.types._

    val locationSchema = StructType(List(
      StructField("sid", LongType, nullable = true),
      StructField("id", StringType, nullable = true),
      StructField("name", StringType, nullable = true),
      StructField("street", StringType, nullable = true),
      StructField("zip", StringType, nullable = true),
      StructField("city", StringType, nullable = true),
      StructField("region", StringType, nullable = true),
      StructField("cd", StringType, nullable = true),
      StructField("phone", StringType, nullable = true),
      StructField("aj_exact_city_match", StringType, nullable = true),
      StructField("aj_exact_country_match", StringType, nullable = true),
      StructField("blurb", StringType, nullable = true),
      StructField("dir_city_id", StringType, nullable = true),
      StructField("dir_city_name", StringType, nullable = true),
      StructField("dir_city_slug", StringType, nullable = true),
      StructField("dir_country_id", StringType, nullable = true),
      StructField("dir_country_name", StringType, nullable = true),
      StructField("lat", StringType, nullable = true),
      StructField("lng", StringType, nullable = true),
      StructField("primary_alias_on_fb", StringType, nullable = true),
      StructField("slug", StringType, nullable = true),
      StructField("website", StringType, nullable = true),
      StructField("cts", StringType, nullable = true)
    ))

    val mostcitiesSchema = StructType(List(
      StructField("city", StringType, nullable = true),
      StructField("QuantityOfAccount", IntegerType, nullable = true)))


    //CONVERT JSON TO DF
    val instagramSchemadf = InstagramDf.select(from_json($"value".cast("string"), locationSchema).alias("instagramlocation")).select("instagramlocation.*")
    instagramSchemadf.printSchema

    import org.apache.spark.sql.streaming.OutputMode.Complete

    // Aggregate for city, quantity of accounts per city
    val mostCitiesDf = instagramSchemadf
      .groupBy("city")
      .agg(count("id").as("QuantityOfAccount"))
      .sort($"QuantityOfAccount".desc)

     mostCitiesDf.printSchema()


    val InstagramDfquery = mostCitiesDf
      .selectExpr("CAST(city AS STRING)","CAST(QuantityOfAccount AS INTEGER)","to_json(struct(*)) AS value")
      .writeStream
      .format("kafka")
      .option("checkpointLocation", "path/to/HDFS/dir")
      .option("failOnDataLoss", "false")
      .outputMode(Complete)
      .option("kafka.bootstrap.servers", brokers)
      .option("topic", "most-cities-account")
      .start()
    

    InstagramDfquery.awaitTermination()
    spark.stop()

  }

}
