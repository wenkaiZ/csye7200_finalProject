import org.apache.spark.sql.{SparkSession}
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

    // Evaluate the model with RMSE
    // Evaluate the model on rating data
    val usersProducts = ratingsRDD.map { case Rating(user, product, rate) =>
      (user, product)
    }
    val predictions =
      model.predict(usersProducts).map { case Rating(user, product, rate) =>
        ((user, product), rate)
      }
    val ratesAndPreds = ratingsRDD.map { case Rating(user, product, rate) =>
      ((user, product), rate)
    }.join(predictions)
    val MSE = ratesAndPreds.map { case ((user, product), (r1, r2)) =>
      val err = (r1 - r2)
      err * err
    }.mean()

    println(s"Mean Squared Error = $MSE")
//    val rmseTest = computeRmse(model, testRDD, true)
//    println("Test RMSE: = " + rmseTest) //Less is better

    println("End")
  }

  def computeRmse(model: MatrixFactorizationModel, data: RDD[Rating], implicitPrefs: Boolean)
  : Double = {

    def mapPredictedRating(r: Double): Double = {
      if (implicitPrefs) math.max(math.min(r, 1.0), 0.0) else r
    }

    val predictions: RDD[Rating] = model.predict(data.map(x => (x.user, x.product)))
    val predictionsAndRatings = predictions.map{ x =>
      ((x.user, x.product), mapPredictedRating(x.rating))
    }.join(data.map(x => ((x.user, x.product), x.rating))).values
    math.sqrt(predictionsAndRatings.map(x => (x._1 - x._2) * (x._1 - x._2)).mean())
  }

  def sparkInit(name: String): SparkSession = {
    SparkSession
      .builder
      .appName(name)
      .config("spark.master", "local")
      .getOrCreate()
  }
}
