package setup

import com.datastax.driver.core._
import com.datastax.driver.core.Cluster
import scala.collection.JavaConversions._


object Setup {
  def runThenClose(f: => Unit) {
    val csc = new CassandraDataSetup("localhost")
    csc.populateTable()
    csc.querySchema
    try f
    finally csc.close
  }
}


class CassandraDataSetup(node: String) {

  protected val cluster  = Cluster.builder.addContactPoint(node).build
  protected val metaData = cluster.getMetadata
  protected val session  = cluster.connect

  createSchema("test.persons")
  createSchema("test.adults" )
  createSchema("test.minors" )

  val sourceBS = bs("test.persons")
  val adultsBS = bs("test.adults" )
  val minorsBS = bs("test.minors" )

  protected def bs(tableName: String) = new BoundStatement(
    session.prepare(s"""INSERT INTO $tableName (id, fname, lname, age) VALUES (?, ?, ?, ?)""")
  )

  println(s"Connected to cluster: ${metaData.getClusterName}")

  metaData.getAllHosts.foreach{ h => println(
    s"Datacenter: ${h.getDatacenter}; Host: ${h.getAddress}, Rack: ${h.getRack}"
  )}

  def createSchema(tableName: String) {

    session.execute("""CREATE KEYSPACE IF NOT EXISTS test WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};""")

    session.execute(s"""
        |CREATE TABLE IF NOT EXISTS $tableName (
        | id text PRIMARY KEY,
        | fname text,
        | lname text,
        | age int
        |);
      """.stripMargin)

    session.execute(s"CREATE INDEX IF NOT EXISTS ON $tableName (fname);")
    session.execute(s"CREATE INDEX IF NOT EXISTS ON $tableName (lname);")
    session.execute(s"CREATE INDEX IF NOT EXISTS ON $tableName (age);"  )
  }

  def populateTable(implicit bs: BoundStatement = sourceBS) {
    insert(java.util.UUID.randomUUID.toString, "Matt",  "Kew", 40)
    insert(java.util.UUID.randomUUID.toString, "Anita", "Kew", 35)
    insert(java.util.UUID.randomUUID.toString, "Gavin", "Kew",  6)
    insert(java.util.UUID.randomUUID.toString, "Aries", "Kew",  3)
  }

  def insert(id: String, fname: String, lname: String, age: java.lang.Integer)(implicit bs: BoundStatement) {
    session.execute(bs.bind(id, fname, lname, age))
  }

  def querySchema {
    val results = session.execute("""SELECT * FROM test.persons WHERE fname = 'Matt';""")
    println(String.format("%-30s\t%-20s\t%-20s\n%s", "fname", "lname", "age", "-------------------------------+-----------------------+--------------------"))
    results.foreach{ row => println(String.format("%-30s\t%-20s\t%-20s", row.getString("fname"), row.getString("lname"),  row.getInt("age").toString)) }
  }

  def close {
    session.execute("DROP TABLE test.persons")
    session.execute("DROP TABLE test.adults" )
    session.execute("DROP TABLE test.minors" )
    cluster.close
  }
}
