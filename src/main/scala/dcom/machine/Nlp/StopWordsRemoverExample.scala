package dcom.machine.Nlp

import org.apache.spark.ml.feature.StopWordsRemover
import dcom.SessionGenerator
// $example off$
import org.apache.spark.sql.SparkSession

object StopWordsRemoverExample {
  def main(args: Array[String]): Unit = {
    val spark = SessionGenerator.getSparkSession

    // $example on$
    val remover = new StopWordsRemover()
      .setInputCol("raw")
      .setOutputCol("filtered")

    val dataSet = spark.createDataFrame(Seq(
      (0, Seq("I", "saw", "the", "red", "balloon")),
      (1, Seq("Mary", "had", "a", "little", "lamb"))
    )).toDF("id", "raw")

    remover.transform(dataSet).show(false)
    // $example off$

    spark.stop()
  }
}
// scalastyle:on println
