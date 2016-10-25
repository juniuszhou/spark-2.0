package src.main.scala

import java.io.{File, FileOutputStream, PrintWriter}

/**
  * Created by junius on 16-10-18.
  */
object SimpleApp {
  def main(args: Array[String]) {
    val writer = new PrintWriter(new File("data/userMovieTest.csv"))
    (1 to 5).foreach(i => {
      (1 to 5).foreach(j => {
        writer.append(i.toString + "," + j + ",0\n")
      })
    })
    writer.close()

  }
}
