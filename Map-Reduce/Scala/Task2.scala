package src.main.scala

import java.io.File


import org.apache.spark.sql.{DataFrame, Dataset, SQLContext, SparkSession}
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD._
import java.io
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.rdd.RDD

object Task2 extends App {

  class SimpleCSVHeader(header: Array[String]) extends Serializable {
    val index = header.zipWithIndex.toMap

    def apply(array: Array[String], key: String): String = array(index(key))
  }

  val ratings = args(0)
  val tags = args(1)
  val output = args(2)
  val spark = SparkSession.builder.master("local[*]")
    .appName("sMovieRatingApp")
    .getOrCreate()
  //val conf = new SparkConf().setAppName("MovieRatingsApp").setMaster("local[2]")
  //val sc = new SparkContext(sparkSession)


  import spark.implicits._
  val sc =spark.sparkContext

  val rdd1 = sc.textFile(ratings)
  val rdd2 = sc.textFile(tags)

  val r1: DataFrame =spark.read.option("header","true").csv(ratings)

  val r2: DataFrame =spark.read.option("header","true").option("encoding", "UTF-8").option("quote","\"").option("escape","\"")
    .option("mode","PERMISSIVE")
  .option("treatEmptyValuesAsNulls","true").csv(tags)

  val dftoRdd1 = r1.rdd
  val dftoRdd2 = r2.rdd
  //val rdd_noheader= rdd1.filter((line: String) => !(line startsWith "userId"))
  //  val data1 = rdd1.map(line => line.split(",").map(elem => elem.trim)) //lines in rows
  //val header1 = new SimpleCSVHeader(data1.take(1)(0)) // we build our header with the first line
  //val rows1 = data1.filter(line => header1(line, "userId") != "userId")

  //val data2 = rdd2.map(line => line.split(",").map(elem => elem.trim)) //lines in rows
  //val header2 = new SimpleCSVHeader(data2.take(1)(0)) // we build our header with the first line
  //val rows2 = data2.filter(line => header2(line, "userId") != "userId")



  val ratingRdd = r1.rdd.map(rating => (rating(1).toString.toInt, rating(2).toString.toFloat)).cache()
  val tagsRdd = r2.rdd.map(row => (row(1).toString.toInt, row(2).toString.toLowerCase)).cache()
  //rows2.map(tag => (tag(1).toInt, tag(2))).cache()



  val avgRdd= ratingRdd.mapValues(x => (x, 1)).
    reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)).mapValues(y => 1.0 * y._1 / y._2).sortByKey(true)

  val newRdd = tagsRdd.join(avgRdd).map(x => ( x._2._1 , x._2._2))

  val tagavgRdd= newRdd.mapValues(x => (x, 1)).
    reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)).mapValues(y => 1.0 * y._1 / y._2).sortByKey(false)

  /*val res= tagavgRdd.map{case(a, b) =>
    var line = a + "," + b
    line
  }*/

  //val header: RDD[String] = sc.parallelize(Array("tag,rating_avg"))
 // header.union(res).saveAsTextFile(output)
  tagavgRdd.toDF("tag","rating_avg").write.
    format("com.databricks.spark.csv").
    option("header", true).
    option("delimiter", ",").
    option("encoding", "UTF-8").option("quote","\"").option("escape","\"").
    save(output)



  /*val dir = new File(output)
  val newFileRgex = output + File.separatorChar + ".part-0.*.csv"
  val tmpTsvFile = dir.listFiles.filter(_.toPath.toString.matches(newFileRgex))(0).toString
  (new File(tmpTsvFile)).renameTo(new File(output))

  dir.listFiles.foreach( f => f.delete )
  dir.delete*/

}
