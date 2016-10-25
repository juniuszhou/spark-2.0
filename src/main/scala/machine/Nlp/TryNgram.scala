package src.main.scala.machine.Nlp

import org.apache.spark.ml.feature.NGram
import src.main.scala.SessionGenerator

object TryNgram {
  def main(args: Array[String]): Unit = {
    val spark = SessionGenerator.getSparkSession

    val wordDataFrame = spark.createDataFrame(Seq(
      (0, Array("Hi", "I", "heard", "about", "Spark")),
      (1, Array("I", "wish", "Java", "could", "use", "case", "classes")),
      (2, Array("Logistic", "regression", "models", "are", "neat"))
    )).toDF("id", "words")

    val ngram = new NGram().setN(2).setInputCol("words").setOutputCol("ngrams")

    val ngramDataFrame = ngram.transform(wordDataFrame)

    // after training, ngram data frame also include input data columns.
    ngramDataFrame.printSchema()
    ngramDataFrame.select("ngrams").show(false)

    spark.stop()
  }
}
