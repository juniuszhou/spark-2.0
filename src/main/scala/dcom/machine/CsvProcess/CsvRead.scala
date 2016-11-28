package dcom.machine.CsvProcess

import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.{DataFrame, SparkSession}
import dcom.SetLogLevel


object CsvRead {

  // read csv file with all data are number, include both label as first column
  // and others are features.
  def readCsv(sparkSession: SparkSession, path: String, header: Boolean): DataFrame = {
    import sparkSession.implicits._

    val df = sparkSession.read.option("header", header).csv(path)
    //val df = sparkSession.read.format("csv").option("header", false)
    df.map(row => {
      val seq = row.toSeq.tail.map(an => an.toString.toDouble)
      val v = Vectors.dense(seq.toArray)
      (row.getString(0).toDouble, v)
    }).toDF("label", "features")
  }

  // read csv file for clustering algorithm without label.
  // but label is must be item for spark ml. so we can give it a dummy value.
  def readNoLabelCsv(sparkSession: SparkSession, path: String): DataFrame = {
    import sparkSession.implicits._
    val df = sparkSession.read.csv(path)
    df.map(row => {
      val array = row.toSeq.map(an => an.toString.toDouble).toArray
      val v = Vectors.dense(array)
      (0L, v)
    }).toDF("label", "features")
  }

  def readUserMovieCsv(sparkSession: SparkSession, path: String): DataFrame = {
    import sparkSession.implicits._
    val df = sparkSession.read.csv(path)
    df.map(row => {
      (row.getString(0).toInt, row.getString(1).toInt, row.getString(2).toDouble)
    }).toDF("userId", "movieId", "rating")
  }

  def main(args: Array[String]) {
    SetLogLevel.setLogLevels()
    val session = SparkSession.builder()
      .master("local[2]").appName("MemberJoin").getOrCreate()
    import session.implicits._

    val path = "data/sample.csv"
    val df = readCsv(session, path, header = true)
    df.printSchema()
    df.foreach(row => println(row.toString()))

    val df2 = readNoLabelCsv(session, path)
    df2.printSchema()
    session.stop()

  }
}
