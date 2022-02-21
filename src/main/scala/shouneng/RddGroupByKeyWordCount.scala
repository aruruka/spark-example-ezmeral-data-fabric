package shouneng

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RddGroupByKeyWordCount {
  try {
    val conf =
      new SparkConf().setAppName("RddGroupByKeyWordCount").setMaster("local[4]")
    val sparkContext = new SparkContext(conf)

    val rootPath: String = "file://"
    val file: String =
      s"${rootPath}/home/shouneng/geektime/Getting_Started_with_Spark/chapter01/wikiOfSpark.txt"

    val lineRDD: RDD[String] = sparkContext.textFile(file)
    val wordRDD: RDD[String] = lineRDD.flatMap(line => line.split(" "))
    val cleanWordRDD: RDD[String] = wordRDD.filter(word => !word.equals(""))
    val kvRDD: RDD[(String, String)] = cleanWordRDD.map(word => (word, word))
    
    val words: RDD[(String, Iterable[String])] = kvRDD.groupByKey()
  } catch {
    case ex: Exception => {
      ex.printStackTrace()
      System.err.println("exception===>: ...")
    }
  }
}
