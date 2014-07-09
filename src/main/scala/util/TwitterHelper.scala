package util

//::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
//::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::

// NOTE!
// The contents of this file was taken (and altered) from TwitterHelper found in an ampcamp4 training exercise:
// see https://github.com/amplab/training

//::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
//::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::


import java.util

import org.apache.spark.streaming._
import org.apache.spark.storage.StorageLevel
import scala.io.Source
import scala.collection.mutable.HashMap
import java.io.File
import org.apache.log4j.Logger
import org.apache.log4j.Level
import sys.process.stringSeqToProcess

object TwitterHelper {
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  Logger.getLogger("org.apache.spark.streaming.NetworkInputTracker").setLevel(Level.INFO)

  /** Configures the Oauth Credentials for accessing Twitter */
  def configureTwitterCredentials() {
    val file = new File("twitter.txt")
    if (!file.exists) {
      throw new Exception("Could not find configuration file " + file)
    }
    val lines = Source.fromFile(file.toString).getLines.filter(_.trim.size > 0).toSeq
    val pairs = lines.map(line => {
      val splits = line.split("=")
      if (splits.size != 2) {
        throw new Exception("Error parsing configuration file - incorrectly formatted line [" + line + "]")
      }
      (splits(0).trim(), splits(1).trim())
    })
    val map = new HashMap[String, String] ++= pairs
    val configKeys = Seq("consumerKey", "consumerSecret", "accessToken", "accessTokenSecret")
    println("Configuring Twitter OAuth")
    configKeys.foreach(key => {
      if (!map.contains(key)) {
        throw new Exception("Error setting OAuth authenticaion - value for " + key + " not found")
      }
      val fullKey = "twitter4j.oauth." + key
      System.setProperty(fullKey, map(key))
      println("\tProperty " + fullKey + " set as " + map(key))
    })
    println()
  }
}

