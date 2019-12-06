import org.apache.spark.sql.SparkSession
import org.apache.spark.mllib.evaluation.RegressionMetrics
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.rdd.RDD

object ALSRecommendation {
  def main(args: Array[String]): Unit = {
    println("Loading data...")

    val spark = sparkInit("ALS Recommendation")

    val rating_df = spark.read
      .format("csv")
      .option("header", "true")
      .csv("./src/main/resources/ratings.csv")

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
      .setBlocks(block)
      .setAlpha(alpha)
      .setLambda(lambda)
      .setRank(rank)
      .setSeed(seed)
      .setImplicitPrefs(implicitPrefs)
      .run(ratingsRDD)

    // Get top 6 products predictions for user 333
    println("Rating:(UserID, AppID, Rating)")
    println("----------------------------------")
    val topRecsForUser = model.recommendProducts(333, 6)
    for (rating <- topRecsForUser) {
      println(rating.toString()) }
    println("----------------------------------")

    // Evaluate the model with MSE on rating data
    val MSE = computeMse(model, testRDD)
    println(s"Mean Squared Error = $MSE")

    // Evaluate the model by computing the RMSE on the test data
    val RMSE = computeRmse(model, testRDD)
    println(s"Root Mean Square Error = $RMSE")

    println("End")
  }

  def computeMse(model: MatrixFactorizationModel, data: RDD[Rating]): Double = {
    val usersProducts = data.map {
      case Rating(user, product, _) => (user, product)
    }

    val predictions = model.predict(usersProducts).map {
      case Rating(user, product, rate) => ((user, product), rate)
    }

    val ratesAndPreds = data.map {
      case Rating(user, product, rate) => ((user, product), rate)
    }.join(predictions)

    ratesAndPreds.map {
      case ((_, _), (r1, r2)) => val err = r1 - r2
      err * err
    }.mean()
  }

  def computeRmse(model: MatrixFactorizationModel, data: RDD[Rating]): Double = {
    // Get predictions for each data point
    val predictions = model.predict(data.map(
      r => (r.user, r.product))).map(
      r => ((r.user, r.product), r.rating))
    val allRatings = data.map(
      r => ((r.user, r.product), r.rating))
    val ratesAndPreds = predictions.join(allRatings).map {
      case ((_, _), (predicted, actual)) => (predicted, actual) }

    // Get the RMSE using regression metrics
    val regressionMetrics = new RegressionMetrics(ratesAndPreds)
    regressionMetrics.meanSquaredError
  }

  def sparkInit(name: String): SparkSession = {
    SparkSession
      .builder
      .appName(name)
      .config("spark.master", "local")
      .getOrCreate()
  }
}
