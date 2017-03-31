package dcom

import org.apache.spark.sql.SparkSession

object CallLdap {
  def main(args: Array[String]) {
    val session = SparkSession.builder().master("local[2]").appName("aa").getOrCreate()
    val data = session.sparkContext.parallelize((0 to 10).toList, 10)
    val df = data.mapPartitionsWithIndex((index, par) => {
      (0 to 1000).toList.map(i => index * 100000 + i).iterator
    })
    df.collect().foreach(println)
    session.stop()
  }
}
