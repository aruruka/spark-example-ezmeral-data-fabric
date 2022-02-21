package shouneng

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RddAggregateByKeyWordCount extends App {
  val conf =
    new SparkConf().setAppName("NeighboringWordCount").setMaster("local[4]")
  val sparkContext = new SparkContext(conf)

  val rootPath: String = "file://"
  val file: String =
    s"${rootPath}/home/shouneng/geektime/Getting_Started_with_Spark/chapter01/wikiOfSpark.txt"

  val lineRDD: RDD[String] = sparkContext.textFile(file)
  val wordRDD: RDD[String] = lineRDD.flatMap(line => line.split(" "))
  val cleanWordRDD: RDD[String] = wordRDD.filter(word => !word.equals(""))
  val kvRDD: RDD[(String, Int)] = cleanWordRDD.map(word => (word, 1))

  def f1(x: Int, y: Int): Int = {
    return x + y
  }

  def f2(x: Int, y: Int): Int = {
    return math.max(x, y)
  }

  val wordCounts: RDD[(String, Int)] = kvRDD.aggregateByKey(0)(f1, f2)

  wordCounts.sortByKey(false).take(5).foreach(println)

  sparkContext.stop()
}
