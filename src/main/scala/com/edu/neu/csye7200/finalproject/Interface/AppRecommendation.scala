package com.edu.neu.csye7200.finalproject.Interface

import java.sql.Date

import com.edu.neu.csye7200.finalproject.configure.FileConfig
import com.edu.neu.csye7200.finalproject.util.{ALSUtil, DataUtil, QueryUtil}
import com.github.tototoshi.csv._


object AppRecommendation {
  lazy val df=DataUtil.getAppsDF

  /**
    * This function trained the data and get the recommendation application
    * for specific user and print the result.
    * @param userId     The specific user of recommendation
    * @return           Return the RMSE and improvement of the Model
    */
  def getRecommendation(userId: Int) = {
    //RDD[long, Rating]
    val ratings = DataUtil.getAllRating(FileConfig.ratingFile)

    val appsArray = DataUtil.getAppsArray

    val links = DataUtil.getLinkData(FileConfig.linkFile)
    val apps = DataUtil.getCandidatesAndLink(appsArray, links)

    val userRatingRDD = DataUtil.getRatingByUser(FileConfig.ratingFile, userId)
    val userRatingApp = userRatingRDD.map(x => x.product).collect
      val numRatings = ratings.count()
      val numUser = ratings.map(_._2.user).distinct().count()
      val numApp = ratings.map(_._2.product).distinct().count()

      println("rating: " + numRatings + " apps: " + numApp
        + " user: " + numUser)

      // Split data into train(60%), validation(20%) and test(20%)
      val numPartitions = 10

      val trainSet = ratings.filter(x => x._1 < 6).map(_._2).
        union(userRatingRDD).repartition(numPartitions).persist()
      val validationSet = ratings.filter(x => x._1 >= 6 && x._1 < 8)
        .map(_._2).persist()
      val testSet = ratings.filter(x => x._1 >= 8).map(_._2).persist()

      val numTrain = trainSet.count()
      val numValidation = validationSet.count()
      val numTest = testSet.count()

      println("Training data: " + numTrain + " Validation data: " + numValidation
        + " Test data: " + numTest)

      //      Train model and optimize model with validation set
      ALSUtil.trainAndRecommendation(trainSet, validationSet, testSet, apps, userRatingRDD)
    }

    def queryBySelectedInAppsJson(content: String, SelectedType: String) = {
      QueryUtil.QueryAppJson(df, content, SelectedType)
    }

    def queryBySeletedInAppsNormal(content: String, SelectedType: String) = {
      QueryUtil.QueryAppInfoNorm(df, content, SelectedType)
    }

    def queryByKeywords(content: String) = {
      //Query of keywords
      val keywordsRDD = DataUtil.getKeywords(FileConfig.keywordsFile)
      QueryUtil.QueryOfKeywords(keywordsRDD, df, content)
    }  

  def SortBySelected(ds:Array[(Int,String,String,String,Date,Double)],selectedType:String="popularity",order:String="desc")= {
    selectedType match {
      case "popularity" => order match {
        case "desc" => ds.sortBy(-_._6)
        case "asc" => ds.sortBy(_._6)
      }
      case "release_date" =>
        order match {
          case "desc" => ds.sortWith(_._5.getTime > _._5.getTime)
          case _ => ds.sortBy(-_._5.getTime)
        }
    }
  }

    def queryBystaffInCredits(content: String, SelectedType: String) = {
      QueryUtil.QueryOfstaff(DataUtil.getStaff(FileConfig.creditFIle), df, content, SelectedType)
    }

  /**
    * Search apps by movie name
    * @param AppName    The user input of app name
    * @return             Option of Array of [Int] contains the app tmdbID
    */
    def FindAppByName(AppName: String) = {
      val id = QueryUtil.QueryAppIdByName(df, AppName).map(x => x._1)
      if (id.length != 0)
        Some(id)
      else None
    }


    def UpdateRatingsByRecommendation(RatingsInfo: List[String], AppName: String) = {
      val appId = FindAppByName(AppName).getOrElse(Array())
      val writer = CSVWriter.open(FileConfig.ratingFile, append = true)
      if (appId.nonEmpty) {
        val links = DataUtil.getLinkData(FileConfig.linkFile)
        val imdbId = DataUtil.movieIdTransfer(appId, links)
        writer.writeRow(insert(RatingsInfo, 1, imdbId(0)))
        println("Rating Successfully")
      }
      else println("Cannot find the app you entered")
      writer.close()

    }

    def insert(list: List[String], i: Int, value: Int) = {
      list.take(i) ++ List(value.toString) ++ list.drop(i)
    }
  }


