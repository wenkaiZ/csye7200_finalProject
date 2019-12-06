package com.edu.neu.csye7200.finalproject.Interface

import com.edu.neu.csye7200.finalproject.configure.FileConfig
import com.edu.neu.csye7200.finalproject.util.DataUtil

object AppRecommendation {
  lazy val appDF = DataUtil.getAppDF

  /**
    * This function trained the data and get the recommendation movie
    * for specific user and print the result.
    * @param userId     The specific user of recommendation
    * @return           Return the RMSE and improvement of the Model
    */
  def getRecommendation(userId: Int) ={
    val ratings = DataUtil.getAllRating(FileConfig.ratingFile)

  }

}
