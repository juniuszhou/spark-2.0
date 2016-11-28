package dcom

import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by junius on 16-10-8.
  */
object SessionGenerator {
  def getSparkSession = {
    SetLogLevel.setLogLevels()
    val spark = SparkSession
      .builder
      .master("local[2]")
      .appName("MemberJoin")
      .getOrCreate()

    spark
  }

  def getYarnSparkSession = {
    SetLogLevel.setLogLevels()
    val spark = SparkSession
      .builder
      .appName("MemberJoin")
      .getOrCreate()

    spark
  }

  def main(args: Array[String]) {

  }
}
