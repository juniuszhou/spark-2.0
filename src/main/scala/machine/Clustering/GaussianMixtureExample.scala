package src.main.scala.machine.Clustering

import org.apache.spark.ml.clustering.GaussianMixture
import org.apache.spark.sql.SparkSession
import src.main.scala.SessionGenerator
import src.main.scala.machine.CsvProcess.CsvRead

object GaussianMixtureExample {
  def main(args: Array[String]): Unit = {
    val spark = SessionGenerator.getSparkSession
    val data = CsvRead.readNoLabelCsv(spark, "data/no_label.csv")

    // set how many classes we expect.
    val gmm = new GaussianMixture().setK(2)
    val model = gmm.fit(data)

    for (i <- 0 until model.getK) {
      // weight represent how important of a gaussian model. even we don't need set any
      // parameter when training. we can get a different weight according to data.
      // mean foreach model centroid value
      // cov covariance matrix of features.
      println(s"Gaussian $i:\nweight=${model.weights(i)}\n" +
        s"mu=${model.gaussians(i).mean}\nsigma=\n${model.gaussians(i).cov}\n")
    }


    spark.stop()
  }
}
