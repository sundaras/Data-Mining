package src.main.scala
import java.io.File

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD._
import org.apache.spark.sql.SparkSession


object Task1 extends App {

  class SimpleCSVHeader(header: Array[String]) extends Serializable {
    val index = header.zipWithIndex.toMap

    def apply(array: Array[String], key: String): String = array(index(key))
  }

  val ratings = args(0)
  val output = args(1)

  val spark = SparkSession.builder.
    master("local[*]")
    .appName("sMovieRatingApp")
    .getOrCreate()
  //val conf = new SparkConf().setAppName("MovieRatingsApp").setMaster("local[2]")
  //val sc = new SparkContext(sparkSession)


  import spark.implicits._
  val sc =spark.sparkContext

  val rdd1 = sc.textFile(ratings)

  //val rdd_noheader= rdd1.filter((line: String) => !(line startsWith "userId"))
  val data = rdd1.map(line => line.split(",").map(elem => elem.trim)) //lines in rows
  val header = new SimpleCSVHeader(data.take(1)(0)) // we build our header with the first line
  val rows = data.filter(line => header(line, "userId") != "userId")

  val ratingRdd = rows.map(rating => (rating(1).toInt, rating(2).toFloat)).cache()


  val avgRdd: RDD[(Int, Double)] = ratingRdd.mapValues(x => (x, 1)).
    reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)).mapValues(y => 1.0 * y._1 / y._2).sortByKey(true)

  /*avgRdd.map{case(a, b) =>
    var line = a + "," + b
    line
  }.saveAsTextFile(output);
*/


  avgRdd.toDF("movieId","rating_avg").repartition(1).write.
    format("com.databricks.spark.csv").
    option("header", true).
    option("delimiter", ",").
    save(output)


  /*val dir = new File(output)
  val newFileRgex = output + File.separatorChar + ".part-0.*.csv"
  val tmpTsvFile = dir.listFiles.filter(_.toPath.toString.matches(newFileRgex))(0).toString
  (new File(tmpTsvFile)).renameTo(new File(output))

  dir.listFiles.foreach( f => f.delete )
  dir.delete*/
}