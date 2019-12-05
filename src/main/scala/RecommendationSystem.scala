import org.apache.spark.sql.{DataFrame, SparkSession}
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
    val app_df = spark.read
      .format("csv")
      .option("header", "true")
      .schema(apps_schema)
      .csv("./src/main/resources/app_info.csv")
    val numApps = app_df.select("appId").count()

    println("Loading ratings.csv to spark DataFrame...")
    // Recreate schema for ratings DataFrame
    val ratings_schema = StructType(Array(
      StructField("userId", IntegerType, false),
      StructField("appId", IntegerType, false),
      StructField("rating", DoubleType, true),
      StructField("timestamp", StringType, true)
    ))

    val rating_df = spark.read
      .format("csv")
      .option("header", "true")
      .schema(ratings_schema)
      .csv("./src/main/resources/ratings.csv")
    val numRatings = rating_df.count()
    val numUsers = rating_df.select("userId").distinct().count()

    println("Got " + numRatings + " ratings from " + numUsers + " users on " + numApps + " Apps.")

    // Register both DataFrames as temp tables to make querying easier
    app_df.createOrReplaceTempView("apps")
    rating_df.createOrReplaceTempView("ratings")

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
    val splits = rating_df.randomSplit(Array(0.75, 0.25), seed = 12345L)
    val (trainingData, testData) = (splits(0), splits(1))
    val numTraining = trainingData.count()
    val numTest = testData.count()
    println("Training size: " + numTraining + " Testing size: " + numTest)

    // Training RDD contains userId, appId, rating
    val ratingsRDD = trainingData.rdd.map(row => {
      val userId = row.getString(0)
      val appId = row.getString(1)
      val ratings = row.getString(2)
      Rating(userId.toInt, appId.toInt, ratings.toDouble)
    })

    // Test RDD contains the same attributes
    val testRDD = testData.rdd.map(row => {
      val userId = row.getString(0)
      val appId = row.getString(1)
      val ratings = row.getString(2)
      Rating(userId.toInt, appId.toInt, ratings.toDouble)
    })

    // Build an ALS user matrix model based on ratingsRDD with params
    val rank = 20
    val numIterations = 15
    val lambda = 0.10
    val alpha = 1.00
    val block = -1
    val seed = 12345L
    val implicitPrefs = false
    val model = new ALS()
      .setIterations(numIterations)
      .setBlocks(block).setAlpha(alpha)
      .setLambda(lambda)
      .setRank(rank) .setSeed(seed)
      .setImplicitPrefs(implicitPrefs)
      .run(ratingsRDD)

    // Get top 6 products predictions for user 333
    println("Rating:(UserID, AppID, Rating)")
    println("----------------------------------")
    val topRecsForUser = model.recommendProducts(333, 6)
    for (rating <- topRecsForUser) { println(rating.toString()) }
    println("----------------------------------")

    println("End")
  }
}
