package recsys
import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import scala.Tuple2
import scala.List
import java.util._
import org.apache.spark.mllib.recommendation.{ALS,Rating,MatrixFactorizationModel}
object MovLens{
  def main(args:Array[String]){
    val sc = new SparkContext("local[4]", "movlens",System.getenv("SPARK_HOME"))
   // val data = sc.textFile("test.data") //formatted as: user product rating
    val data = sc.textFile("/home/xunw/data/MovieLens/ml-1m/ratings.dat")
    
    val ratings = data.map(_.split("::") match { case Array(user, item, rate,time) =>
    Rating(user.toInt, item.toInt, rate.toDouble)
  })
  
    val rank = 10  //latent factors
    val numIterations = 20
    val alpha = 0.01
    
    val model:MatrixFactorizationModel= ALS.train(ratings, rank, numIterations, alpha)
    //val model:MatrixFactorizationModel = ALS.trainImplicit(ratings, rank, numIterations)
  //  val model2 = ALS.
    val userProducts = ratings.map{case Rating(user,product,rate) => (user,product)}
    val predictions = model.predict(userProducts).map{case Rating(user,product,rate) => ((user,product),rate)}
    val ratesAndPreds= predictions.join(ratings.map{
      case Rating(user,product,rate) => ((user,product),rate)}
    )
    
    val MSE = ratesAndPreds.map { case ((user, product), (r1, r2)) => 
    val err = (r1 - r2)
     err * err
     }.mean()
     
    println("Mean Squared Error = " + MSE)
    
   
    println(ratings.count())
  }

}