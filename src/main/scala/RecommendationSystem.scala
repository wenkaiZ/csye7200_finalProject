import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, DoubleType, StringType, StructField, StructType}

object RecommendationSystem {
  def main(args: Array[String]) {
    // Create the entry point to programming with Spark
    val spark = SparkSession
      .builder
      .appName("Recommendation System")
      .config("spark.master", "local")
      .getOrCreate()

    println("Start processing: ")
    // Read csv files to DataFrame
//    val df1 = spark.read
//      .format("csv")
//      .option("header", "true")
//      .load("./src/main/resources/AppleStore.csv")
//      .select("id","track_name", "user_rating")
//    df1.show(10)
//    df1.printSchema()

    val df2 = spark.read
      .format("csv")
      .option("header", "true")
      .load("./src/main/resources/googleplaystore_user_reviews.csv")
    df2.show(100)
    println(df2.count())
    df2.select("App").distinct().show(100)
    // df2.createOrReplaceTempView("google")
    df2.printSchema()

    println("End")
  }
}
