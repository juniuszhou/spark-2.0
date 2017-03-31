package dcom.sql

import org.apache.spark.sql.SparkSession

object simply {
  def main(args: Array[String]) {
    val session = SparkSession.builder().master("local[2]").getOrCreate()
    // if create data frame from list or seq, at least two items for each row.
    val list = List((0,1))
    val df = session.createDataFrame(data = list)
    val df3 = df.toDF("id", "age")
    df3.select("id").printSchema()
    val df2 = session.read.text("hdfs://10.104.90.40:8020/user/zhoujun04/python/word.py")
    df2.rdd.map(row => row.getString(0)).foreach(str => println(str))
    //df.foreach(row)
    session.read.parquet("").foreach(row => println(row))
    session.stop()
  }
}