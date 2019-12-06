package com.edu.neu.csye7200.finalproject

import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}
import org.scalatest.tagobjects.Slow
import com.edu.neu.csye7200.finalproject.Interface.AppRecommendation
import com.edu.neu.csye7200.finalproject.util._
import scala.util.Random
class QuerySpec extends FlatSpec with Matchers with BeforeAndAfter {

  before {
  val df = DataUtil.getAppsDF
  }
  behavior of "Spark Query "

  it should "work for query  United States of America  in production_countries " taggedAs Slow in{
    val content="America"
    val SelectedType="production_countries"
    Random.shuffle(AppRecommendation.queryBySelectedInAppsJson(content,SelectedType).toSeq).take(5).filter(_._2.contains(content)).size should matchPattern{
      case 5=>
    }
  }


}
