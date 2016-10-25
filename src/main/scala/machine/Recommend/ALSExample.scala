package src.main.scala.machine.Recommend

import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.ALS
import src.main.scala.SessionGenerator
import src.main.scala.machine.CsvProcess.CsvRead

object ALSExample {
  case class Rating(userId: Int, movieId: Int, rating: Float, timestamp: Long)
  def parseRating(str: String): Rating = {
    val fields = str.split("::")
    assert(fields.size == 4)
    Rating(fields(0).toInt, fields(1).toInt, fields(2).toFloat, fields(3).toLong)
  }
  // $example off$

  def main(args: Array[String]) {
    val spark = SessionGenerator.getSparkSession
    import spark.implicits._

    // $example on$
    val training = CsvRead.readUserMovieCsv(spark, "data/userMovie.csv")
    val test = CsvRead.readUserMovieCsv(spark, "data/userMovieTest.csv")
    //val Array(training, test1) = ratings.randomSplit(Array(0.9, 0.1))

    // Build the recommendation model using ALS on the training data
    val als = new ALS()
      .setMaxIter(5)
      .setRegParam(0.01)
      .setUserCol("userId")
      .setItemCol("movieId")
      .setRatingCol("rating")
    val model = als.fit(training)

    // Evaluate the model by computing the RMSE on the test data
    val predictions = model.transform(test)

    println("test result.")
    predictions.foreach(row => println(row))
    println("--------------------------------")
    // print out two vector got from training.
    model.userFactors.foreach(row => println(row))
    println("---------------------------------")
    model.itemFactors.foreach(row => println(row))

    // compute rmse on test data.
    val evaluator = new RegressionEvaluator()
      .setMetricName("rmse")
      .setLabelCol("rating")
      .setPredictionCol("prediction")
    val rmse = evaluator.evaluate(predictions)
    println(s"Root-mean-square error = $rmse")
    // $example off$

    spark.stop()
  }
}
