package dcom.machine.Similarity

import org.apache.spark.mllib.linalg.distributed.RowMatrix
import dcom.SessionGenerator
import dcom.machine.CsvProcess.CsvRead

object Cosine {
  def main(args: Array[String]) {
    val spark = SessionGenerator.getSparkSession
    import spark.implicits._

//    // doesn't work the row matrix based on rdd and mllib implementation.
//    val data = CsvRead.readCsv(spark, "data/sample.csv", header = false)
//      .map(row => row.getAs[org.apache.spark.mllib.linalg.Vector](1))
//
//    val mat = new RowMatrix(data.rdd)
//
//    // Compute similar columns perfectly, with brute force.
//    val exact = mat.columnSimilarities()
//
//    // Compute similar columns with estimation using DIMSUM
//    val threshold = 0.01
//    val approx = mat.columnSimilarities(threshold)
//
//    exact.entries.foreach(matrix => println(matrix))
//
//    approx.entries.foreach(matrix => println(matrix))



  }
}
