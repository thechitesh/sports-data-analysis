package com.sports.data.analysis

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

object GameAnalysis {

  private val renamedColumns: List[Column] = List(
    col("c_competition").as("Competition"),
    col("n_TeamID").as("TeamId"),
    col("c_Team").as("TeamName"),
    col("d_date").as("MatchDate"),
    col("n_Matchid").as("MatchId"),
    col("c_person").as("PlayerName"),
    col("n_personid").as("PlayerId"),
    col("n_ShirtNr").as("ShirtNumber"),
    col("c_action").as("Action"),
    col("c_Function").as("Function")
  )

  def showMatchDetails(dataFrame: DataFrame): DataFrame = {

    val matchInfo = dataFrame.select(renamedColumns: _*)
      .where(col("Action").equalTo("Line-up"))
      .groupBy("MatchId", "TeamId", "TeamName", "MatchDate")
      .agg(collect_list("PlayerName").alias("Line up"), collect_list("Function").alias("Functions"))
      .as("matchInfo")

    matchInfo
      .orderBy("MatchId", "TeamId")
      .withColumn("TeamName", regexp_replace(col("TeamName"), "NULL", "Support Staff"))
  }


  def showLeagueTable(dataFrame: DataFrame) = {
    val leagueTable = dataFrame.select(renamedColumns: _*)
      .where(col("Action").equalTo("Goal"))
      .groupBy("MatchId","TeamId", "TeamName")
      .agg(count("Action").as("Goal"))
      .as("gameScore")

    leagueTable
      .groupBy("MatchId", "TeamName")
      .agg(max("Goal").as("Total Goals"))
      .sort(col("Total Goals").desc)
      .as("League")
  }


  def showGameScore(dataFrame: DataFrame): DataFrame = {
    dataFrame.select(renamedColumns: _*)
      .where(col("Action").equalTo("Goal"))
      .groupBy("MatchId","TeamId", "TeamName")
      .agg(count("Action").as("Goal"))
      .orderBy("MatchId", "TeamId")
      .as("gameScore")
  }

  def showPlayerDetails(dataFrame: DataFrame): DataFrame = {

    val goals = dataFrame
      .select(renamedColumns: _*)
      .where(col("Action").equalTo("Goal"))
      .groupBy("PlayerId")
      .agg(count("Action").alias("Goals"))
      .alias("goals")

    val players = dataFrame
      .select(renamedColumns: _*)
      .where(!col("PlayerName").equalTo("NULL"))
      .where(!col("ShirtNumber").equalTo("NULL"))
      .groupBy("PlayerId", "PlayerName", "ShirtNumber", "TeamName")
      .agg(collect_list("Function").alias("Functions"))
      .alias("Players")

    val playerDetails = players.join(goals, Seq("PlayerId"), "left")
      .alias("playerDetails")
    playerDetails.orderBy("ShirtNumber")
  }

}
