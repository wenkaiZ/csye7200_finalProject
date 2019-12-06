package com.edu.neu.csye7200.finalproject.util

import org.apache.spark.sql.SparkSession
import com.edu.neu.csye7200.finalproject.Schema._
import com.edu.neu.csye7200.finalproject.configure.FileConfig


object DataUtil {
  lazy val spark = sparkInit("AppRecommendation")

  lazy val appDF = spark.read
    .format("csv")
    .option("header", "true")
    .schema(Schemas.apps_schema)
    .csv(FileConfig.appFile)

  def getAllRating = {
    spark.read
      .format("csv")
      .option("header", "true")
      .csv(FileConfig.ratingFile)
  }

  def getAppsArray = {
    appDF.select("appId", "appName").collect().filter(_(0) != null).map(x => (x.getInt(0), x.getString(1)))
  }

  def getAppDF = appDF

  def sparkInit(name: String): SparkSession = {
    SparkSession
      .builder
      .appName(name)
      .config("spark.master", "local")
      .getOrCreate()
  }
}
