package recsys
import java.util.Random
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import org.apache.spark.mllib.recommendation.{ALS, Rating, MatrixFactorizationModel}
import org.apache.spark.Partition

object Test {
  def main(args:Array[String]){
    val sc = new SparkContext("local[4]", "movlens",System.getenv("SPARK_HOME"))
    val rdds = sc.parallelize(1 to 10000,4)
    val partioners = rdds.partitioner
    val rdds2 = rdds.repartition(2)
    println(partioners)
    println(rdds.partitions.length)
    println(rdds2.partitions(0).index)
//    var sum  = sc.accumulator(0) ;
//    rdds.foreach(i => { sum +=i })
//    println(sum)
  }

}