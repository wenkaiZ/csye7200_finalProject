import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}
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
    val apps_schema = StructType(Array(
      StructField("appId", IntegerType, false),
      StructField("app_name", StringType, false),
      StructField("category", StringType, true),
      StructField("rating", StringType, true),
      StructField("reviews", IntegerType, true),
      StructField("type", StringType, true),
      StructField("price", StringType, true),
      StructField("content_rating", StringType, true),
      StructField("last_updated", StringType, true)
    ))

    // Read csv files to DataFrame
    val df1 = spark.read
      .format("csv")
      .option("header", "true")
      .schema(apps_schema)
      .csv("./src/main/resources/app_info.csv")
    val numApps = df1.select("appId").count()

    println("Loading ratings.csv to spark DataFrame...")
    // Recreate schema for ratings DataFrame
    val ratings_schema = StructType(Array(
      StructField("userId", IntegerType, false),
      StructField("appId", IntegerType, false),
      StructField("rating", DoubleType, true),
      StructField("timestamp", StringType, true)
    ))

    val df2 = spark.read
      .format("csv")
      .option("header", "true")
      .schema(ratings_schema)
      .csv("./src/main/resources/ratings.csv")
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
      "select apps.app_name, apprates.maxr, apprates.minr, apprates.count "
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
        + "apps.app_name, ratings.rating FROM ratings JOIN apps "
        + "ON ratings.appId=apps.appId "
        + "WHERE ratings.userId=414 AND ratings.rating > 4")
    results2.show(false)

    // Find the top 50 most reviews dating apps from apps table
    val result3 = spark.sql(
      "SELECT appId, app_name, rating, reviews FROM apps "
        + "ORDER BY reviews DESC")
    result3.show(50)

    // Prepare training and test rating data and check the counts

    println("End")
  }
}
