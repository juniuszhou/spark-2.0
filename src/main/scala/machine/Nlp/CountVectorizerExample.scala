package machine.Nlp

import org.apache.log4j.Logger
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import src.main.scala.SetLogLevel
// $example off$
import org.apache.spark.sql.SparkSession

object CountVectorizerExample {
  def main(args: Array[String]) {
    SetLogLevel.setLogLevels()
    val spark = SparkSession
      .builder
      .appName("CountVectorizerExample")
      .master("local[4]")
      .getOrCreate()

    val df = spark.createDataFrame(Seq(
      (0, Array("a", "b", "c")),
      (1, Array("a", "b", "b", "c", "a"))
    )).toDF("id", "words")

    // fit a CountVectorizerModel from the corpus
    val cvModel: CountVectorizerModel = new CountVectorizer()
      .setInputCol("words")
      .setOutputCol("features")
      .setVocabSize(3)
      .setMinDF(2)
      .fit(df)

    // alternatively, define CountVectorizerModel with a-priori vocabulary
    val cvm = new CountVectorizerModel(Array("a", "b", "c"))
      .setInputCol("words")
      .setOutputCol("features")

    print(cvModel.vocabulary)
    print(cvModel)

    val result = cvModel.transform(df)
    result.printSchema()
    result.foreach(row => println(row.getAs[StructType](2)))
    result.select("features").foreach(row => println)

    // $example off$

    spark.stop()
  }
}
// scalastyle:on println



