package recsys
import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import scala.Tuple2
import scala.List
import java.util._
object MovLens{
  def main(args:Array[String]){
    println("hello:")
    val sc = new SparkContext("local[4]", "movlens",System.getenv("SPARK_HOME"))
    val arr = sc.parallelize(List(1,2,3,4), 4)
    
    println(arr.sum())
  }

}