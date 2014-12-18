import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import org.apache.spark.rdd.JdbcRDD
import java.sql.{ Connection, DriverManager, ResultSet }

object SparkCopyMySQL {

  case class Account(user_id: String, account_id: String, balance: Double)

  def main(args: Array[String]) {
    val sparkConf = new SparkConf(true)
      .set("spark.cassandra.connection.host", "127.0.0.1")

    val sc = new SparkContext("spark://127.0.0.1:7077", "SparkSync", sparkConf)

    val url = "jdbc:mysql://192.168.25.172:3306/testdb"
    val user = "root"
    val password = "elephant"

    Class.forName("com.mysql.jdbc.Driver").newInstance

    val myRDD = new JdbcRDD(sc, () => DriverManager.getConnection(url, user, password), 
        "select * from accounts limit ?, ?", 0, 1000000, 10, 
        r => new Account(r.getString("user_id"), r.getString("account_id"), r.getDouble("balance")))

    println("Processing Table")
    myRDD.collect.foreach(println)
  }
}









