import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

/**
 * Application that syncs phone numbers between a master and replica system. 
 * 
 * HDFS is being used at the system of record and Cassandra as the front end web application. When we validate
 * the data in HDFS we want to insert the updated phone numbers in Cassandra but only if they haven't changed 
 * in the time between them being sent to the master system and when the validation process runs. 
 */
object SparkSync {

  case class User(id: java.util.UUID, first: String, last: String, email: String, country: String, phone: String);

  /**
   * Update the user using lightweight transactions (LWT)
   */
  def updateUser(newUser: User, oldUser: User, sc: SparkContext): Boolean = {
    val oldPhone = oldUser.phone
    val newPhone = newUser.phone;

    val resultSet = CassandraConnector(sc.getConf).withSessionDo { session =>
      session.execute("update test.users set phone='" + newPhone + "' where id = " + oldUser.id + " if phone='" + oldPhone + "' ");
    }
        
    val row = resultSet.one
    val result = row.getBool(0)
    
    if (result) {
      println("updated " + oldUser.id + " with " + newPhone);
    } else {
      println("Could not update " + oldUser.id + " with " + newPhone + " as new phone no is " + row.getString(1));
    }
    result
  }

  /**
   * For each user, new and old, update in Cassandra
   */
  def updateUsers(newRows: Map[java.util.UUID, User], oldRows: Map[java.util.UUID, User], sc: SparkContext): Int = {
    newRows.foreach { case (a, b) => val oldUser = oldRows(a); updateUser(b, oldUser, sc); }
    return 1;
  }
  
  /** 
   *  For testing purposes this runs on the spark master node.
   */
  def main(args: Array[String]) {
    
    val hdfsFile = args(0)
        
    val sparkConf = new SparkConf(true)
      .set("spark.cassandra.connection.host", "127.0.0.1")
      .set("spark.cores.max", "2")

    val sc = new SparkContext("spark://127.0.0.1:7077", "SparkSync", sparkConf)

    //Read from hdfs
    val usersHdfs = sc.textFile(hdfsFile).cache

    //Create users from our file
    val usersDL = usersHdfs.map(line => line.trim().split(",") match { 
      case Array(id, first, last, email, country, phone) => 
        new User(java.util.UUID.fromString(id), first, last, email, country, phone) })

    //Rows that are invalid
    val rowsToUpdate = usersDL.filter { user => user.phone.contains("2222") }.cache

    println(rowsToUpdate.count + " users need updating");

    //Update the users details to correct value
    val newRows = rowsToUpdate.map {
      case (user) => new User(user.id, user.first,
        user.last, user.email, user.country, user.phone.replaceAll("2222", "0000"))
    }.cache

    //Map the rows by their key
    val oldRowsMap = rowsToUpdate.keyBy(f => f.id).collect.toMap
    val newRowsMap = newRows.keyBy(f => f.id).collect.toMap

    updateUsers(newRowsMap, oldRowsMap, sc);
  }
}




