import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType, DoubleType, DateType}
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.rdd.RDD

object RecommendationSystem {
  def main(args: Array[String]) {
    println("Creating Spark Session: ")

    // Create the entry point to programming with Spark
    val spark = SparkSession
      .builder
      .appName("Recommendation System")
      .config("spark.master", "local")
      .getOrCreate()

    println("Start processing: ")
    println("Loading app_info.csv to spark DataFrame...")

    // Recreate schema for apps DataFrame
//    val apps_schema = StructType(Array(
//      StructField("appId", IntegerType, false),
//      StructField("app_name", StringType, false),
//      StructField("category", StringType, true),
//      StructField("rating", DoubleType, true),
//      StructField("reviews", IntegerType, true),
//      StructField("Type", StringType, true),
//      StructField("price", DoubleType, true),
//      StructField("content_rating", StringType, true),
//      StructField("last_updated", DateType, true)
//    ))

    // Read csv files to DataFrame
    val df1 = spark.read
      .format("csv")
      .option("header", "true")
      .load("./src/main/resources/app_info.csv")
    val numApps = df1.select("appId").count()
//    df1.withColumnRenamed("App", "app_name")
//      .withColumnRenamed("Category", "category")
//      .withColumnRenamed("Rating", "rating")
//      .withColumnRenamed("Reviews", "reviews")
//      .withColumnRenamed("Type", "type")
//      .withColumnRenamed("Price", "price")
//      .withColumnRenamed("Content Rating", "content_rating")
//      .withColumnRenamed("Last Updated", "last_updated")
    df1.show(10)
    df1.printSchema()

    println("Loading ratings.csv to spark DataFrame...")
    val df2 = spark.read
      .format("csv")
      .option("header", "true")
      .load("./src/main/resources/ratings.csv")
    val numRatings = df2.count()
    val numUsers = df2.select("userId").distinct().count()

    println("Got " + numRatings + " ratings from " + numUsers + " users on " + numApps + " Apps.")

    // Register both DataFrames as temp tables to make querying easier
    df1.createOrReplaceTempView("apps")
    df2.createOrReplaceTempView("ratings")

    println("Getting some insights...")
    // Get the max, min ratings along with the count of users who have rated a product
    println("Get the app names with max, min ratings along with the count of users:")
    val results = spark.sql(
      "select apps.App, apprates.maxr, apprates.minr, apprates.count "
      + "from(SELECT ratings.appId,max(ratings.rating) as maxr,"
      + "min(ratings.rating) as minr,count(distinct userId) as count "
      + "FROM ratings group by ratings.appId) apprates "
      + "join apps on apprates.appId=apps.appId "
      + "order by apprates.count desc")
    results.show(false)

    // Find the 10 most active users and how many times they rated a product
    println("Find the 10 most active users and how many times they rated apps:")
    val mostActiveUsersSchemaRDD = spark.sql(
      "SELECT ratings.userId, count(*) as count from ratings "
        + "group by ratings.userId order by count desc limit 10")
    mostActiveUsersSchemaRDD.show(false)

    // Look at a particular user and find the apps that user rated higher than 4
    val results2 = spark.sql(
      "SELECT ratings.userId, ratings.appId,"
        + "apps.App, ratings.rating FROM ratings JOIN apps "
        + "ON ratings.appId=apps.appId "
        + "WHERE ratings.userId=414 AND ratings.rating > 4")
    results2.show(false)

    // Find the top 10 average rating dating apps from apps table
//    val result3 = spark.sql(
//      "SELECT appId, App, rating, reviews FROM apps "
//        + "ORDER BY rating, reviews DESC")
//    result3.show(20)

    // Prepare training and test rating data and check the counts

    println("End")
  }
}
