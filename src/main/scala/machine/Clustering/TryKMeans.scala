package src.main.scala.machine.Clustering

import org.apache.spark.ml.clustering.KMeans
import src.main.scala.SessionGenerator
import src.main.scala.machine.CsvProcess.CsvRead

/**
  * Created by junius on 16-10-9.
  */
object TryKMeans {
  def main(args: Array[String]) {
    val spark = SessionGenerator.getSparkSession
    //val data = spark.read.format("libsvm").load("data/mllib/sample_kmeans_data.txt")

    val data = CsvRead.readNoLabelCsv(spark, "data/no_label.csv")
    val kmeans = new KMeans().setFeaturesCol("features").setK(2)

    val model = kmeans.fit(data)
    // print out all centroid info.
    model.clusterCenters.foreach(vector => println(vector))

    val cost = model.computeCost(data)
    println("cost is " + cost)
    spark.stop()

  }
}
