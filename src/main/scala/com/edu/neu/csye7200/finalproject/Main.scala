package com.edu.neu.csye7200.finalproject
import com.edu.neu.csye7200.finalproject.Interface.AppRecommendation
import scala.util.control.Breaks._
import scala.util.{Failure, Success, Try}

object Main extends App {
  def ToInt(line: String): Try[Int] = {
    Try(line.toInt)
  }

  override def main(args: Array[String]): Unit = {
    breakable {
      while (true) {
        println(
          "\n/-----------------------------------------------------------------------/" +
          "\n/-------------------- Welcome to AppRecommendation ---------------------/" +
          "\n/-----------------------------------------------------------------------/" +
          "\n/-------------------- Please Log In By Enter UserId --------------------/" +
          "\n/-----------------------------------------------------------------------/" +
          "\n/------------------------- Input exit to Exit --------------------------/"
        )
        val id = scala.io.StdIn.readLine()
        if (id.equals("exit")) break
        breakable {
          while (true) {
            ToInt(id) match {
              case Success(t) => {

                println(
                    "\n/-----------------------------------------------------------------------/" +
                    "\n/-------------------- Welcome to AppRecommendation ---------------------/" +
                    "\n/-----------------------------------------------------------------------/" +
                    "\n                             User ID:  " + id +
                    "\n/-----------------------------------------------------------------------/" +
                    "\n/------------------------- Input exit to Exit --------------------------/"
                )
                println("1.App Recommendation" +
                  "\n2.App Search" +
                  "\n3.App Rating(give better recommendation for you)")
                var  num =scala.io.StdIn.readLine()
                if(num.equals("exit")) break
                breakable {
                  while (true) {
                    ToInt(num) match {
                      case Success(t)=>{
                        t match{
                          case 1=>{ println(
                              "\n/-----------------------------------------------------------------------/" +
                              "\n/-------------------- Welcome to AppRecommendation ---------------------/" +
                              "\n/-----------------------------------------------------------------------/" +
                              "\n                             User ID:  " + id +
                              "\n/-----------------------------------------------------------------------/" +
                              "\n/------------------------- Input exit to Exit --------------------------/"
                          )
                            AppRecommendation.getRecommendation(id.toInt)
                            break
                          }
                          case 2=>{
                            breakable {
                              while (true) {
                                println(
                                  "\n/-----------------------------------------------------------------------/" +
                                  "\n/------------------------- App Search ----------------------------------/" +
                                  "\n/-----------------------------------------------------------------------/" +
                                  "\n                             User ID:  " + id +
                                  "\n/-----------------------------------------------------------------------/" +
                                  "\n/--------------------- Input exit to Exit ------------------------------/"
                                )
                                println("1.Search by Keywords")
                                num = scala.io.StdIn.readLine()
                                if(num.equals("exit")) break
                                ToInt(num) match {

                                  case Success(v) => {
                                    println("Enter Content")
                                    val content=scala.io.StdIn.readLine()
                                    v match{
                                      case 1 => AppRecommendation.queryBySelectedInAppsJson(content, "production_countries").sortWith(_._6 > _._6).take(5).foreach(line => println("id: " + line._1, "popularity: " +
                                        line._6, "name: " + line._3, "production_coutries: " + line._2, "tagline: " + line._4))
                                      case _=>break
                                    }

                                  }
                                  case Failure(e)=> break
                                }
                              }
                            }
                          }
                          case 3=>{
                            println(

                                "\n/-----------------------------------------------------------------------/" +
                                "\n/------------------------- App Rating ----------------------------------/" +
                                "\n/-----------------------------------------------------------------------/" +
                                "\n                             User ID:  " + id +
                                "\n/-----------------------------------------------------------------------/" +
                                "\n/---------------- Enter the App name you wanna rate --------------------/" +
                                "\n/-----------------------------------------------------------------------/" +
                                "\n/--------------------- Input exit to Exit ------------------------------/"
                            )
                            val content=scala.io.StdIn.readLine()
                            if(content.equals("exit")) break
                            breakable {
                              while (true) {
                                println("Enter rating you wanna give(0~5)")
                                val rating=scala.io.StdIn.readLine()
                                if(rating.equals("exit")) break
                                Try(rating.toFloat) match{
                                  case Success(r)=> {
                                    if(r>=0&&r<=5) {
                                      AppRecommendation.UpdateRatingsByRecommendation(List(id.toInt.toString, r.toString,
                                        (System.currentTimeMillis()%10000000000.00).toLong.toString), content)
                                      break
                                    }
                                    else {
                                      println("out of range")
                                    }
                                  }
                                  case Failure(r)=>
                                }

                              }
                            }
                          }
                          case _=>break
                        }

                      }
                      case Failure(e)=>break
                    }
                  }
                }
              }
              case Failure(e)=> break
            }

          }
        }
      }
    }
  }
}
