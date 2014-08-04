package sql
import java.util.Random
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import org.apache.spark.sql.SQLContext
import org.apache.spark.mllib.recommendation.{ALS, Rating, MatrixFactorizationModel}
case class Person(name: String, age: Int)
object HelloSql {
  def main(args:Array[String]){
   val sc = new SparkContext("local[4]", "movlens",System.getenv("SPARK_HOME"))
   val sqlContext = new org.apache.spark.sql.SQLContext(sc)
   import sqlContext.createSchemaRDD

// Create an RDD of Person objects and register it as a table.
   val people = sc.textFile("/home/xunw/spark/examples/src/main/resources/people.txt").map(_.split(",")).map(p => Person(p(0), p(1).trim.toInt))
   val schemaRDD = sqlContext.createSchemaRDD(people)
   schemaRDD.registerAsTable("people")
   
   val teenagers = sqlContext.sql("SELECT name FROM people WHERE age >= 13 AND age <= 19")
   teenagers.map(t => "Name: " + t(0)).collect().foreach(println)
  }
   
}