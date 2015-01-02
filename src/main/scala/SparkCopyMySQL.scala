import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import org.apache.spark.rdd.JdbcRDD
import java.sql.{ Connection, DriverManager, ResultSet }

object SparkCopyMySQL {

  case class Account(user_id: String, account_id: String, balance: Double)
  
  case class User(id: java.util.UUID, first: String, last: String, email: String, country: String, phone: String);

  def main(args: Array[String]) {
    val sparkConf = new SparkConf(true)
      .set("spark.cassandra.connection.host", "127.0.0.1")

    val sc = new SparkContext("spark://127.0.0.1:7077", "SparkSync", sparkConf)

    val url = "jdbc:mysql://localhost:3306/testdb"
    val user = "root"
    val password = ""

    Class.forName("com.mysql.jdbc.Driver").newInstance

    val accountsRDD = new JdbcRDD(sc, () => DriverManager.getConnection(url, user, password), 
        "select * from accounts limit ?, ?", 0, 10000, 3, 
        r => new Account(r.getString("user_id"), r.getString("account_id"), r.getDouble("balance")))

    println("Processing Account Table")
    accountsRDD.partitions.foreach(println)
    //accountsRDD.collect.foreach(println)
    println(accountsRDD.collect.toList.size)

    val usersRDD = new JdbcRDD(sc, () => DriverManager.getConnection(url, user, password), 
        "select * from users limit ?, ?", 0, 100000000, 10, 
        r => new User(java.util.UUID.fromString(r.getString("id")), r.getString("first"), r.getString("last"), r.getString("email"), r.getString("country"), r.getString("phone")))

    println("Processing User Table")
    usersRDD.partitions.foreach(println)
    //usersRDD.collect.foreach(println)
    println(usersRDD.collect.toList.size)
    
    println("Copying User Table")
    usersRDD.saveToCassandra("test", "users")
    println("Copied User Table to Cassandra")
  }
}








	