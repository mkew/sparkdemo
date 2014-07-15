import com.datastax.driver.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import setup.Setup


object BasicCassandraAndSpark extends App {

  case class Person(id: String, fname: String, lname: String, age: Int)

  Setup.runThenClose {

    val conf = new SparkConf(true).set("cassandra.connection.host", "127.0.0.1")
    implicit val sc = new SparkContext("local", "test", conf)

    val persons = sc.cassandraTable[Person]("test", "persons")
    val adults = persons.filter(_.age > 17)
    adults.saveToCassandra("test", "adults"); printAdults

    sc.stop()
  }

  def printAdults(implicit sc: SparkContext) = print(sc.cassandraTable[Person]("test", "adults"))("Adults")

  def print[T](rdd: RDD[T])(title: String, line: String = ":" * 50) {
    println(s"""
        |$line
        |Total $title: ${rdd.count()}
        |${rdd.collect().mkString("\n")}
        |$line""".stripMargin
    )
  }
}
