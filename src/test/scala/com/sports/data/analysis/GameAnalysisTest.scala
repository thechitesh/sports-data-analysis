package com.sports.data.analysis

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Encoders, Row, SparkSession}
import org.scalatest.funsuite.AnyFunSuite

class GameAnalysisTest extends AnyFunSuite {

  private val sparkSession = SparkSession.builder()
    .appName("Game Analysis Test")
    .master("local[*]")
    .getOrCreate()


  private val schema = StructType(Seq(
    StructField("n_actionid", StringType, true),
    StructField("c_competition", StringType, true),
    StructField("n_Matchid", StringType, true),
    StructField("d_date", StringType, true),
    StructField("c_Action", StringType, true),
    StructField("c_Period", StringType, true),
    StructField("n_TeamID", StringType, true),
    StructField("c_Team", StringType, true),
    StructField("n_personid", StringType, true),
    StructField("c_person", StringType, true),
    StructField("c_Function", StringType, true),
    StructField("n_ShirtNr", StringType, true),
    StructField("c_Subperson", StringType, true)
  ))

  test("Show Match Details") {
    println("hello")
    val testData = Seq(
      Row("22039489", "Test competition", "2174508", "11-Aug-2017 19:00", "Line-up", "NULL","16", "FC Utrecht", "924566", "Anouar Kali", "NULL", "1223164", "Sheraldo Becker"),
      Row("22039489", "Test competition", "2174508", "11-Aug-2017 19:00", "Line-up", "NULL","16", "ADO Den Haag", "664080", "Lex Immers", "NULL", "NULL", "NULL"),
      Row("22039450", "Test competition", "2174508", "11-Aug-2017 19:00", "Line-up", "NULL","16", "ADO Den Haag", "664080", "Lex Immers", "NULL", "NULL", "NULL")
    )
    implicit val encoder = Encoders.row(schema)
    val testDF = sparkSession.createDataset(testData)
    val result = GameAnalysis.showMatchDetails(testDF).collectAsList()
    assert(result.size() == 2)
  }

  test("show League Table ") {
    val testData = Seq(
      Row("22039489", "Test competition", "2174508", "11-Aug-2017 19:00", "Goal", "NULL","16", "FC Utrecht", "924566", "Anouar Kali", "NULL", "1223164", "Sheraldo Becker"),
      Row("22049499", "Test competition", "2174508", "11-Aug-2017 19:00", "Goal", "NULL","16", "FC Utrecht", "924566", "Donny Gorter", "NULL", "1223164", "Sheraldo Becker"),
      Row("22049499", "Test competition", "2174508", "11-Aug-2017 19:00", "Goal", "NULL","16", "FC AMC", "924566", "J Jammy", "NULL", "1223164", "Sheraldo Becker"),
      Row("22039289", "Test competition", "2274528", "11-Aug-2017 19:00", "Goal", "NULL","26", "ADO Den Haag", "664080", "Trevor David", "NULL", "NULL", "NULL"),
      Row("22029450", "Test competition", "2174528", "11-Aug-2017 19:00", "Free kick", "NULL","34", "PSV", "664080", "Nick Marsman", "NULL", "NULL", "NULL")
    )
    implicit val encoder = Encoders.row(schema)
    val testDF = sparkSession.createDataset(testData)
    val result :java.util.List[Row]= GameAnalysis.showLeagueTable(testDF).collectAsList()
    assert(result.size() == 3)
    assert(result.get(0).get(2) == 2)
  }

  test("show Game Score ") {
    val testData = Seq(
      Row("22039489", "Test competition", "2174508", "11-Aug-2017 19:00", "Goal", "NULL","16", "FC Utrecht", "924566", "Anouar Kali", "NULL", "1223164", "Sheraldo Becker"),
      Row("22049499", "Test competition", "2174508", "11-Aug-2017 19:00", "Goal", "NULL","16", "FC Utrecht", "924566", "Donny Gorter", "NULL", "1223164", "Sheraldo Becker"),
      Row("22049499", "Test competition", "2174508", "11-Aug-2017 19:00", "Goal", "NULL","16", "FC AMC", "924566", "J Jammy", "NULL", "1223164", "Sheraldo Becker"),
      Row("22039289", "Test competition", "2274528", "11-Aug-2017 19:00", "Goal", "NULL","26", "ADO Den Haag", "664080", "Trevor David", "NULL", "NULL", "NULL"),
      Row("22029450", "Test competition", "2174528", "11-Aug-2017 19:00", "Free kick", "NULL","34", "PSV", "664080", "Nick Marsman", "NULL", "NULL", "NULL")
    )
    implicit val encoder = Encoders.row(schema)
    val testDF = sparkSession.createDataset(testData)
    val result :java.util.List[Row]= GameAnalysis.showGameScore(testDF).collectAsList()
    assert(result.size() == 3)
    assert(result.get(0).get(2) == "FC Utrecht")
  }


  test("show Player Details ") {
    val testData = Seq(
      Row("22039489", "Test competition", "2174508", "11-Aug-2017 19:00", "Goal", "NULL","16", "FC Utrecht", "924566", "Anouar Kali", "NULL", "10", "Sheraldo Becker"),
      Row("22049499", "Test competition", "2174508", "11-Aug-2017 19:00", "Goal", "NULL","16", "FC Utrecht", "924566", "Anouar Kali", "NULL", "10", "Sheraldo Becker"),
      Row("22049499", "Test competition", "2174508", "11-Aug-2017 19:00", "Goal", "NULL","16", "FC AMC", "924567", "J Jammy", "NULL", "13", "Sheraldo Becker"),
      Row("22039289", "Test competition", "2274528", "11-Aug-2017 19:00", "Goal", "NULL","26", "ADO Den Haag", "664030", "Trevor David", "NULL", "14", "NULL"),
      Row("22029450", "Test competition", "2174528", "11-Aug-2017 19:00", "Free kick", "NULL","34", "PSV", "664080", "Nick Marsman", "NULL", "15", "NULL")
    )
    implicit val encoder = Encoders.row(schema)
    val testDF = sparkSession.createDataset(testData)
    val result :java.util.List[Row]= GameAnalysis.showPlayerDetails(testDF).collectAsList()
    assert(result.size() == 4)
    assert(result.get(0).get(1) == "Anouar Kali")
  }

}
