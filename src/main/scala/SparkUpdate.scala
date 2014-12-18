import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object SparkUpdate {

  case class User(id: java.util.UUID, first: String, last: String, email: String, country: String, phone: String);

  def main(args: Array[String]) {
    val sparkConf = new SparkConf(true)
      .set("spark.cassandra.connection.host", "127.0.0.1")

    val sc = new SparkContext("spark://127.0.0.1:7077", "SparkSync", sparkConf)

    //Read from hdfs
    val usersHdfs = sc.textFile("hdfs://192.168.25.179:8020/user/users.csv")
    
    println("Processing file")

    //Create users from our file
    val usersDL = usersHdfs.map(line => line.trim().split(",") match { case Array(id, first, last, email, country, phone) => new User(java.util.UUID.fromString(id), first, last, email, country, phone) })

    //Update the users details to correct value
    val newRows = usersDL.map { case (user) => new User(user.id, user.first, user.last, user.email, 
        user.country, user.phone.replaceAll("1111", "0000")) }.cache

    //print out results
    newRows.foreach(println)    
        
    //write back to hdfs
    newRows.saveAsTextFile("hdfs://192.168.25.179:8020/user/users_updated.csv");
    println("File saved")
  }
}




