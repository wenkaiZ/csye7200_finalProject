package com.edu.neu.csye7200.finalproject.util

import com.edu.neu.csye7200.finalproject.Schema.AppSchema
import com.edu.neu.csye7200.finalproject.configure.FileConfig
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.sql.SparkSession

object DataUtil {

  lazy val spark = SparkSession
    .builder()
    .appName("AppRecommondation")
    .master("local[*]")
    .getOrCreate()
  lazy val appDF=spark.read.option("header", true).schema(AppSchema.appSchema).csv(FileConfig.appFile)

  /**
    * Get RDD object from ratings.csv file which contains all the rating information
    * @param file   The path of the file
    * @return       RDD of [[(Long, Rating)]] with(timestamp % 10, user, product, rating)
    */
  def getAllRating(file: String) = {
    var rating = spark.read.textFile(file)
    val header = rating.first()
    rating = rating.filter(row => row != header)
    rating.rdd.map { line =>
      val fields = line.split(",")
      // (timestamp, user, product, rating)
      (fields(3).toLong%10, Rating(fields(0).toInt, fields(1).toInt, fields(2).toFloat))
    }
  }

  /**
    * Get all the app data of Array type
    * @return       Array of [[(Int, String)]] contiaining (appId, title)
    */
  def getAppsArray  = {
    import spark.implicits._
    // There are some null id in apps data and filter them out
    appDF.select($"id", $"title").collect().filter(_(0) != null).map(x => (x.getInt(0), x.getString(1)))
  }

  /**
    * Get all the app data of DataFrame type
    * @return       DataFrame contain all the information
    */
  def getAppsDF = appDF

  /**
    * Get the rating information of specific user
    * @param file   The path of the file
    * @param userId user Id
    * @return       RDD of[[Rating]] with (user, product, rating)
    */
  def getRatingByUser(file: String, userId: Int) = {
    var rating = spark.read.textFile(file)
    val header = rating.first()
    rating = rating.filter(row => row != header)
    rating.rdd.map { line =>
      val fields = line.split(",")
      // (timestamp, user, product, rating)
      (fields(3).toLong, Rating(fields(0).toInt, fields(1).toInt, fields(2).toDouble))
    }.filter(row => userId == row._2.user)
      .map(_._2)
  }

  /**
    * Get the appId and tmdbId
    * @param file   The path of file
    * @return       Map of [[Int, Int]] with (id and tmdbId)
    */
  def getLinkData(file: String) = {
    val df = spark.read.option("header", true).schema(AppSchema.linkdataSchema).csv(file)
    import spark.implicits._
    // Set tmdbId as the app id and mapping to the id.
    df.select($"appId", $"tmdbId").collect.filter(_(1) != null).map(x => (x.getInt(1), x.getInt(0))).toMap
  }

  /**
    * Get the keywords of apps which keywords formed in JSON format
    * @param file   The path of file
    * @return       DataFrame of keywords
    */
  def getKeywords(file: String) = {
    spark.read.option("header", true).schema(AppSchema.keywordsSchema).csv(file)
  }

  /**
    * Get the keywords of apps which keywords formed in JSON format
    * @param file   The path of file
    * @return       DataFrame of staff
    */
  def getStaff(file:String)={
    spark.read.option("header", true).schema(AppSchema.staffSchema).csv(file)
  }

  /**
    * Get the Candidate apps and replace the id with tmdbId
    * @param apps   Array of [[Int, String]] with (Id, title)
    * @param links   Map of [[Int, Int]] with (appId, imdbId)
    * @return         Map of [[Int, String]]
    */
  def getCandidatesAndLink(apps: Array[(Int, String)], links: Map[Int, Int]) = {
    apps.filter(x => links.get(x._1).nonEmpty).map(x => (links(x._1), x._2)).toMap
  }

  /**
    * Transfer the TMDB ID of app in app_metadata to the
    * app id in rating data
    * @param appIds   The array of TMDBID of apps
    * @param links      Map of [[Int, Int]] with (appId, imdbId)
    * @return           Array of [int] contains corresponding appId
    */
  def movieIdTransfer(appIds: Array[Int], links: Map[Int, Int]) = {
    appIds.filter(x => links.get(x).nonEmpty).map(x => links(x))
  }

}
