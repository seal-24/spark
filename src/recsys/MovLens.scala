package recsys
import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import scala.Tuple2
import scala.List
import java.util._
import org.apache.spark.mllib.recommendation.{ALS,Rating,MatrixFactorizationModel}
object MovLens{
  def recsys(){
    val sc = new SparkContext("local[4]", "movlens",System.getenv("SPARK_HOME"))
   // val data = sc.textFile("test.data") //formatted as: user product rating
    val data = sc.textFile("/home/xunw/data/MovieLens/ml-1m/ratings.dat")
    
    val ratings = data.map(_.split("::") match { case Array(user, item, rate,time) =>
    Rating(user.toInt, item.toInt, rate.toDouble)
  })
  
//    val splits = ratings.randomSplit(Array(0.6, 0.4), seed = 11L)
//    
//    val training = splits(0).cache()        //train
//    val test = splits(1).cache()            //test
    
    val rank = 10  //latent factors
    val numIterations = 20
    val lambda = 0.01
    val alpha = 40
    
    //val model:MatrixFactorizationModel= ALS.train(ratings, rank, numIterations, lambda)
    val model:MatrixFactorizationModel = ALS.trainImplicit(ratings, rank, numIterations,lambda,alpha)
 
//    val userProducts = ratings.map{case Rating(user,product,rate) => (user,product)}
//    val predictions = model.predict(userProducts).map{case Rating(user,product,rate) => ((user,product),rate)}
//    val ratesAndPreds= predictions.join(ratings.map{
//      case Rating(user,product,rate) => ((user,product),rate)}
//    )
//    
//    val MSE = ratesAndPreds.map { case ((user, product), (r1, r2)) => 
//    val err = (r1 - r2)
//     err * err
//     }.mean()
     
    //println("Mean Squared Error = " + MSE)
    
    val up2 = sc.parallelize(List((1,1193),(0,1193),(1,1000000)))
    //println(ratings.count())
    val pred2 = model.predict(up2).collect
    for(r <- pred2){
    printf("%d,%d,%f\n",r.user,r.product,r.rating)
    }
    
    sc.stop()
    
  }
  def main(args:Array[String]){
        
    recsys()


    
  }

}