package com.edu.neu.csye7200.finalproject.Schema

import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}

object Schemas {
  val apps_schema = StructType(
    Array(
      StructField("appId", IntegerType, false),
      StructField("app_name", StringType, false),
      StructField("category", StringType, true),
      StructField("rating", StringType, true),
      StructField("reviews", IntegerType, true),
      StructField("type", StringType, true),
      StructField("price", StringType, true),
      StructField("content_rating", StringType, true),
      StructField("last_updated", StringType, true)
    )
  )

  val ratings_schema = StructType(
    Array(
      StructField("userId", IntegerType, false),
      StructField("appId", IntegerType, false),
      StructField("rating", DoubleType, true),
      StructField("timestamp", StringType, true)
    )
  )

}
