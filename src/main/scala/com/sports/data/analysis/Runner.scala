package com.sports.data.analysis

import com.sports.data.analysis.GameAnalysis.{showGameScore, showLeagueTable, showMatchDetails, showPlayerDetails}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.Using

object Runner {

  def main(args: Array[String]): Unit = {

    Using(startSession) { sparkSession =>

      sparkSession.sparkContext.setLogLevel("ERROR")
      val dataFrame = loadDataFrame(sparkSession)

      val matchDetails = showMatchDetails(dataFrame)
      printResults("Printing Match Details", matchDetails)

      val leagueTable = showLeagueTable(dataFrame)
      printResults("Printing League Table", leagueTable)

      val gameScore = showGameScore(dataFrame)
      printResults("Printing Game Score", gameScore)

      val playerDetails = showPlayerDetails(dataFrame)
      printResults("Printing Player Details", playerDetails)
    }
  }


  private def printResults(message: String, dataFrame: DataFrame) = {
    println("\n\n--------" + message + "---------")
    dataFrame.show(dataFrame.count().toInt, false)
  }

  private def startSession(): SparkSession = {
    SparkSession.builder()
      .appName("scala-sports")
      .master("local[*]") // with * pattern we allow spark to instantiate a local spark session as number of core available on the local machine.
      .getOrCreate()
  }

  private def loadDataFrame(sparkSession: SparkSession): DataFrame = {
    sparkSession.read
      .option("header", true)
      .csv("data/Dataset 2rounds Eredivie 20172018.csv")
  }

}
