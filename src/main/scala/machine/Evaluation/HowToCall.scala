package src.main.scala.machine.Evaluation

import org.apache.spark.ml.evaluation.{BinaryClassificationEvaluator, MulticlassClassificationEvaluator, RegressionEvaluator}
import org.apache.spark.sql.{DataFrame, SparkSession}
import src.main.scala.SetLogLevel
import src.main.scala.machine.CsvProcess.CsvRead

object HowToCall {
  def computeMulti(spark: SparkSession, df: DataFrame): Unit = {
    import spark.implicits._

    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
    // "f1", "weightedPrecision","weightedRecall", "accuracy"
      .setMetricName("accuracy")
    val accuracy = evaluator.evaluate(df)
    println("multi classification accuray is: " + accuracy)
  }

  def computeBinary(spark: SparkSession, df: DataFrame): Unit = {
    import spark.implicits._

    val evaluator = new BinaryClassificationEvaluator()
      .setLabelCol("label")
        .setRawPredictionCol("prediction")
      // "areaUnderRoc" "areaUnderPR"
      .setMetricName("areaUnderROC")
    val accuracy = evaluator.evaluate(df)
    println("binary classification accuray is: " + accuracy)
  }

  def computeRegression(spark: SparkSession, df: DataFrame): Unit = {
    import spark.implicits._

    val evaluator = new RegressionEvaluator()
      .setLabelCol("label")
        .setPredictionCol("prediction")
      // "rmse" "mse" "r2" "mae"
      .setMetricName("rmse")
    val accuracy = evaluator.evaluate(df)
    println("binary classification accuray is: " + accuracy)
  }

  def main(args: Array[String]) {
    SetLogLevel.setLogLevels()
    val spark = SparkSession.builder().master("local[2]")
            .appName("call_evalutor").getOrCreate()
    import spark.implicits._

    // involve first tow columns in accuracy compution
    val df = spark.read.csv("data/sample.csv").toDF("label", "prediction", "dummy", "dummy")
      .map(row => (row.getString(0).toDouble, row.getString(1).toDouble))
      .toDF("label", "prediction")
    computeMulti(spark, df)
    computeBinary(spark, df)
    computeRegression(spark, df)
    spark.stop()

  }
}
