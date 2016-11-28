package dcom.machine.Regression

import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.mllib.evaluation.RegressionMetrics
import dcom.SessionGenerator
import dcom.machine.CsvProcess.CsvRead

object LinearRegre {
  def main(args: Array[String]) {
    val spark = SessionGenerator.getSparkSession
    import spark.implicits._

    val data = CsvRead.readCsv(spark, "data/sample.csv", header = false)
    data.printSchema()
    val regre = new LinearRegression()
      .setFeaturesCol("features")
      .setLabelCol("label")
      .setRegParam(0.2)
      .setElasticNetParam(0.1) // set the weight of L1 penalty.
      .setSolver("auto")  // "l-bfgs", "normal" and "auto"
     val model = regre.fit(data)

    val fullPredictions = model.transform(data).cache()
    val predictions = fullPredictions.select("prediction").rdd.map(_.getDouble(0))
    val labels = fullPredictions.select("label").rdd.map(_.getDouble(0))
    val RMSE = new RegressionMetrics(predictions.zip(labels)).rootMeanSquaredError
    println(s"  Root mean squared error (RMSE): $RMSE")

  }
}
