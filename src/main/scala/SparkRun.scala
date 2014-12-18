
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

/** 
 *  Example of keeping a job running every 10 seconds while sleeping in between. 
 */
object SparkRun {

  //Create Class for existing table
  case class UserInteraction(user_id: String, app: String, time: java.util.Date, action: String)

  //Create Class for the new table
  case class UserInteractionByAction(app: String, action: String, date: String, user_id: String)

  def main(args: Array[String]) {
    val sparkConf = new SparkConf(true)
      .set("spark.cassandra.connection.host", "127.0.0.1")

    val sc = new SparkContext("spark://127.0.0.1:7077", "SparkSync", sparkConf)

    //We only want the date part of the time so that we can filter
    val formatter = new java.text.SimpleDateFormat("dd-MM-yyyy")

    while (true) {

      //Get the data from the existing table
      val interactionsRdd = sc.cassandraTable[UserInteraction]("datastax_user_interactions_demo", "user_interactions")

      println("Printing user interactions");
      //interactionsRdd.collect.foreach(println)
      println(interactionsRdd.count);
      
      println("Sleeping");
      Thread.sleep(10);
    }
  }
}
