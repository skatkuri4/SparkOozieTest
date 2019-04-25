package com.test.spark
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Row
import org.apache.spark.{SparkContext,SparkConf}
object SparkOozieTest {
  case class Person(name: String, age: Int)
  def main(args: Array[String]): Unit =
  {
    val conf: SparkConf =
      new SparkConf()
        .setMaster("yarn-client")
        .setAppName("SparkOozieTest")
    val sc: SparkContext = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    //System.setProperty("hadoop.home.dir","/Users/surenderkatkuri/")
    //val df = sqlContext.read.format("csv").option("delimiter"," ").option("inferSchema","true").load("hdfs://dsrqqa/user/idmaerqt/oozie_test/input/person.txt").as("Person")
    val df = sqlContext.read.text("hdfs://dsrqqa/user/idmaerqt/oozie_test/input/person.txt").toDF()
    df.show()
  }

}
