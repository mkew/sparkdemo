import com.datastax.driver.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import setup.Setup


object CassandraAndSpark extends App {

  case class Person(id: String, fname: String, lname: String, age: Int)

  val vowels = Vector("a", "e", "i", "o", "u")

  Setup.runThenClose {

    val conf = new SparkConf(true).set("cassandra.connection.host", "127.0.0.1")
    implicit val sc = new SparkContext("local", "test", conf)
    val sqlContext = new SQLContext(sc); import sqlContext._

    // Load Person data into a RDD[Person], separate into minors and adults and save them to cassandra and print saved
    val persons = sc.cassandraTable[Person]("test", "persons")
    val adults = persons.filter(_.age > 17)
    val minors = persons.filter(_.age < 18)
    adults.saveToCassandra("test", "adults"); printAdults
    minors.saveToCassandra("test", "minors"); printMinors

    // Count the total number of vowels in the adult and minor first names and print to the console
    def vowelsInFirstName(p: Person) = p.fname.toList.count(l => vowels.contains(l.toString.toLowerCase))
    val numOfAdultFNameVowels = adults.map(vowelsInFirstName).reduce(_ + _)
    val numOfMinorFNameVowels = minors.map(vowelsInFirstName).reduce(_ + _)
    println(s"Adult First Name Vowels: $numOfAdultFNameVowels")
    println(s"Minor First Name Vowels: $numOfMinorFNameVowels")

    // Register minors as a table and run spark SQL queries against it and print the results
    minors.registerAsTable("minors")
    val under5 = sql("SELECT fname, lname FROM minors WHERE age < 5")
    under5.foreach(t => println(s"Under 5: ${t(0)} ${t(1)}"))

    sc.stop()
  }

  def printAdults(implicit sc: SparkContext) = print(sc.cassandraTable[Person]("test", "adults"))("Adults")
  def printMinors(implicit sc: SparkContext) = print(sc.cassandraTable[Person]("test", "minors"))("Minors")

  def print[T](rdd: RDD[T])(title: String, line: String = ":" * 50) {
    println(s"""
        |$line
        |Total $title: ${rdd.count()}
        |${rdd.collect().mkString("\n")}
        |$line""".stripMargin
    )
  }
}
