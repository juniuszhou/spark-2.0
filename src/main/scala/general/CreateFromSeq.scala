package src.main.scala.general

import org.apache.spark.sql.SparkSession

object CreateFromSeq {
  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("createDataFrame").master("local[2]").getOrCreate()

    //for each item, must be tuple more than two elements.
    val df = spark.createDataFrame(List((1,1), (2,2)))
    df.foreach(row => println(row))

    val df2 = spark.createDataFrame(Seq((1, 1), (2, 3)))
    df2.foreach(row => println(row))
  }
}
