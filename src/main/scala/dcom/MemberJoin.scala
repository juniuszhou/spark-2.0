package dcom

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{LogisticRegression, RandomForestClassifier}
import org.apache.spark.ml.evaluation.{BinaryClassificationEvaluator, MulticlassClassificationEvaluator}
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import dcom.machine.CsvProcess.CsvRead

/**
  * Created by junius on 16-10-25.
  */
object MemberJoin {
  def main(args: Array[String]) {
    val spark = SessionGenerator.getYarnSparkSession

    val path = "hdfs://namenode01.bi.10101111.com:8020/user/jzhou/sparkLib/train.csv"
    val data = CsvRead.readCsv(spark, path, header = true)

    val rf = new RandomForestClassifier()

    val pipeline = new Pipeline()
      .setStages(Array(rf))

    val paramGrid = new ParamGridBuilder()
      .addGrid(rf.numTrees, Array(10, 30,100,300,1000))
      .build()

    // We now treat the Pipeline as an Estimator, wrapping it in a CrossValidator instance.
    // This will allow us to jointly choose parameters for all Pipeline stages.
    // A CrossValidator requires an Estimator, a set of Estimator ParamMaps, and an Evaluator.
    // Note that the evaluator here is a BinaryClassificationEvaluator and its default metric
    // is areaUnderROC.
    val cv = new CrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(new MulticlassClassificationEvaluator)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(2)  // Use 3+ in practice

    // Run cross-validation, and choose the best set of parameters.
    val cvModel = cv.fit(data)
    spark.stop()

  }
}
