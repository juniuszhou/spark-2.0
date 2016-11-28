package dcom.machine.Classification

import breeze.stats.distributions.Multinomial
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.{BinaryClassificationEvaluator, MulticlassClassificationEvaluator}
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import dcom.SessionGenerator
import dcom.machine.CsvProcess.CsvRead

/**
  * Created by junius on 16-10-25.
  */
object LogisticR {
  def main(args: Array[String]) {
    val spark = SessionGenerator.getSparkSession
    val Array(data, notused) = CsvRead.readCsv(spark, "/home/junius/mygit/python/Kaggle/digitRecog/input/train.csv", header = true)
      .randomSplit(Array(0.01, 0.99), seed = 1234L)

    // MultinomialLogisticRegression will be supported after 2.1
    // LogisticRegression just support binary cases.
    val lr = new LogisticRegression()
      .setMaxIter(20)
    val pipeline = new Pipeline()
      .setStages(Array(lr))

    val paramGrid = new ParamGridBuilder()
      .addGrid(lr.regParam, Array(0.1, 0.3, 0.03, 0.01))
      .build()

    val cv = new CrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(new MulticlassClassificationEvaluator())
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(2)  // Use 3+ in practice

    // Run cross-validation, and choose the best set of parameters.
    val cvModel = cv.fit(data)
    println(cvModel.toString())

    spark.stop()

  }
}
