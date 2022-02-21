package shouneng

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random._

object RddReduceByKeyWordCount {
  val conf =
    new SparkConf().setAppName("RddReduceByKeyWordCount").setMaster("local[4]")
  val sparkContext = new SparkContext(conf)

  val rootPath: String = "file://"
  val file: String =
    s"${rootPath}/home/shouneng/geektime/Getting_Started_with_Spark/chapter01/wikiOfSpark.txt"

  val lineRDD: RDD[String] = sparkContext.textFile(file)
  val wordRDD: RDD[String] = lineRDD.flatMap(line => line.split(" "))
  val cleanWordRDD: RDD[String] = wordRDD.filter(word => !word.equals(""))
  val kvRDD: RDD[(String, Int)] = cleanWordRDD.map(word => (word, nextInt(100)))

  def f(x: Int, y: Int): Int = {
    return math.max(x, y)
  }

  val wordCounts: RDD[(String, Int)] = kvRDD.reduceByKey(f)
}
