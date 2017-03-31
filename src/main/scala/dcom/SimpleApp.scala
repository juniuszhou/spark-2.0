package dcom

import org.apache.spark.sql.SparkSession
import scala.collection.mutable

object SimpleApp {


  def main(args: Array[String]) {
    val session = SparkSession.builder().master("local[2]").appName("aa").getOrCreate()
    val data = session.sqlContext.read.json("/home/junius/Downloads/hyd")
    // val table2 = session.sqlContext.read.json("path1", "path2")
    val startTime = 0
    val endTime = 149011284500000L
    import session.implicits._
    data.printSchema()
    // data.filter(s"executeTime>=%s and executeTime<=%s".format(startTime, endTime)).foreach(row => println(row))

    val df = data.filter(s"executeTime>=%s and executeTime<=%s".format(startTime, endTime)).map(row => {
      val executeTime = row.getLong(row.fieldIndex("executeTime"))
      val eventType = row.getString(row.fieldIndex("eventType"))
      val rows = row.getStruct(row.fieldIndex("rows"))

      val itemList = rows.getSeq[org.apache.spark.sql.Row](rows.fieldIndex("afterColumns"))

      val map = mutable.Map[String, String]()
      def removeSpecialChar(str: String):String = str.replaceAll("\r\n|\r|\n|\\[|\\]|\001| ", "")
      itemList.map(row => {

        val key = row.getString(row.fieldIndex("name"))
        val value = row.getString(row.fieldIndex("value"))
        map.put(if (key == null) "" else removeSpecialChar(key),
          if (value == null) "" else removeSpecialChar(value))
      })
      itemList.toList
      "s"

    }).count()

    // df.foreach(list => list.foreach(row => println(row)))
    print("Hello world")

  }
}
