package com.sports.data.analysis

import com.sports.data.analysis.GameAnalysis.{showGameScore, showLeagueTable, showMatchDetails, showPlayerDetails}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Runner {

  def main(args: Array[String]): Unit = {
    val matchDetails = showMatchDetails(loadDataFrame())
    printResults(matchDetails)

    val leagueTable = showLeagueTable(loadDataFrame())
    printResults(leagueTable)

    val gameScore = showGameScore(loadDataFrame())
    printResults(gameScore)

    val playerDetails = showPlayerDetails(loadDataFrame())
    printResults(playerDetails)
  }



  private def printResults(dataFrame: DataFrame) = dataFrame.show(dataFrame.count().toInt, false)



  private def loadDataFrame(): DataFrame = {
    val spark: SparkSession = SparkSession.builder()
      .appName("scala-sports")
      .master("local[*]") // with * pattern we allow spark to instantiate a local spark session as number of core available on the machine.
      .getOrCreate()
    spark.read
      .option("header", true)
      .csv("data/Dataset 2rounds Eredivie 20172018.csv")
  }
}
