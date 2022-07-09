package shouneng

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.catalyst.expressions.In

object RddWordCountCollect extends App {

  try {
    val conf = new SparkConf().setAppName("NeighboringWordCount").setMaster("local[4]")
    val sparkContext = new SparkContext(conf)
    val rootPath: String = "file://"
    val file: String = s"${rootPath}/home/raymondyan/geektime/Getting_Started_with_Spark/chapter01/wikiOfSpark.txt"
    // 读取文件内容
    val lineRDD: RDD[String] = sparkContext.textFile(file)

    println(lineRDD.first())

    // 以行为单位做分词
    val wordRDD: RDD[String] = lineRDD.flatMap(line => line.split(" "))
    val cleanWordRDD: RDD[String] = wordRDD.filter(word => !word.equals(""))

    println(cleanWordRDD.take(3))

    // 把RDD元素转换为(Key, Value)的形式
    val kvRDD: RDD[(String, Int)] = cleanWordRDD.map(word => (word, 1))
    // 按照单词做分组计数
    val wordCounts: RDD[(String, Int)] = kvRDD.reduceByKey((x, y) => x + y)

    println(wordCounts.collect())
  } catch {
    case ex: Exception => {
      ex.printStackTrace() // 打印到标准err
      System.err.println("exception===>: ...") // 打印到标准err
    }
  }
}
