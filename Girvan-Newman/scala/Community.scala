package src.main.scala
import java.io.File
import ml.sparkling.graph.operators.OperatorsDSL._
import ml.sparkling.graph.operators.algorithms.community.pscan.PSCAN._
import ml.sparkling.graph.api.operators.algorithms.community.CommunityDetection.ComponentID
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.Edge
import org.apache.spark.graphx.VertexId
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.graphx._
import org.apache.spark.graphx.impl.EdgeRDDImpl
import java.io._


import scala.collection.mutable.ListBuffer

object Community {

  def main(args: Array[String]) = {
    val conf = new SparkConf().setAppName("Bonus").setMaster("local[2]")
    val sc = new SparkContext(conf)
    implicit var ctx: SparkContext = sc
    var movie = sc.textFile(args(0))
    var header = movie.first()

    movie = movie.filter(line => line != header)

    var data = movie.map(_.split(",")).map(line => (line(0).toInt, line(1).toInt)).groupByKey().sortByKey()
      .map { case (x, y) => (x, y.toSet) }.filter { case (x, y) => y.size >= 9 }

    var users = movie.map(_.split(",")).map(line => (line(0).toInt, 1)).distinct().sortByKey()
      .map { case (x, y) => x }

    val u_v: Map[Int, scala.collection.immutable.Set[Int]] = data.collect().toMap


    val vertices: List[PartitionID] = users.collect().toList
    var edges = ListBuffer[(Long, Long)]()
    for (x <- vertices) {

      for (y <- vertices) {
        if (u_v(x).intersect(u_v(y)).size >= 9){
          edges += ((x.toLong, y.toLong))
        }

      }

    }


    var e_rdd: RDD[Edge[Double]] = sc.parallelize(edges).map { case (x, y) => Edge(x, y, 0.0) }
    var edgesRDD: EdgeRDDImpl[Double, Nothing] = EdgeRDD.fromEdges(e_rdd)
    val someRDDArray = users.map{ case x => (x.toLong, x.toLong)}.collect()
    val verticesRDD: RDD[(Long, Long)] = sc.parallelize(someRDDArray)

    println("Length of vertices %d",someRDDArray.length)
    println("Length of edges %d", edges.size)

    var graph = Graph(verticesRDD, edgesRDD)
    val conn_comp: Graph[ComponentID, Double] = graph.PSCAN(epsilon = 0.82)

    val communities = conn_comp.vertices.map(x => x._2-> x._1).groupByKey().sortByKey().map(x => (x._1,x._2.toList.sorted))
    var grps= communities.map(x => x._2).collect()

    print(grps.length)
    var result = ListBuffer[String]()
    for(c <- grps)
    {

      result+= "["+c.mkString(",")+"]"

    }
    val file = new File("Shyamala_Sundararajan_Community.txt")
    val bw = new BufferedWriter(new FileWriter(file))

    for (i <- result)
    {
      bw.write(i)
      bw.write("\n")

    }
    bw.close()
  }
}



