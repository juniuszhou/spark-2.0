package src.main.scala.machine.CsvProcess

import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.{DataFrame, SparkSession}
import src.main.scala.SetLogLevel


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


  def main(args: Array[String]) {
    SetLogLevel.setLogLevels()
    val session = SparkSession.builder()
      .master("local[2]").appName("MemberJoin").getOrCreate()
    import session.implicits._

    val path = "data/sample.csv"
    val df = readCsv(session, path, header = true)
    df.printSchema()
    df.foreach(row => println(row.toString()))
    session.stop()

  }
}
