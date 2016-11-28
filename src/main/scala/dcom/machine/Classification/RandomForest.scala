package dcom.machine.Classification

import dcom.SessionGenerator
import dcom.machine.CsvProcess.CsvRead
import org.apache.spark.ml.classification.RandomForestClassifier

object RandomForest {
  def main(args: Array[String]) {
    val spark = SessionGenerator.getSparkSession
    val data = CsvRead.readCsv(spark, "data/sample.csv", header = false)
    val test = CsvRead.readCsv(spark, "data/sample.csv", header = false).sample(false, 0.1)
    val rf = new RandomForestClassifier()
          .setLabelCol("label")
          .setFeaturesCol("features")
          .setNumTrees(10)

    val model = rf.fit(data)

    // each tree is a decision tree.
    // get all decision trees got after training.
    val trees = model.trees
    trees.foreach(tree => println(tree.toDebugString))

    val prediction = model.transform(test)

    prediction.select("prediction").foreach(row => println(row.get(0)))

    println(model.toDebugString)

    spark.stop()

  }
}
