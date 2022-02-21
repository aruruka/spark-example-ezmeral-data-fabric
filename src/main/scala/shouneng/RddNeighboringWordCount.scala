package shouneng

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object NeighboringWordCount extends App {
  try {
    // When you are on Spark local mode: ⬇️
    val conf = new SparkConf().setAppName("NeighboringWordCount").setMaster("local[4]")
    // When you are on Spark cluster mode: ⬇️
    // val conf = new SparkConf().setAppName("NeighboringWordCount")

    val sparkContext = new SparkContext(conf)

    // When you are on Spark local mode: ⬇️
    val rootPath: String = "file://"
    val file: String =
      s"${rootPath}/home/shouneng/geektime/Getting_Started_with_Spark/chapter01/wikiOfSpark.txt"

    // When you are on Spark cluster mode: ⬇️
    // val rootPath: String = "maprfs://"
    // Put this source file ⬇️ into Ezmeral Data Fabrics's file system using `hadoop fs` command or by POSIX client of the FS or by mounting the EDF NFS storage.
    // val file: String =
    //   s"${rootPath}/user/mapr/wikiOfSpark.txt"

    // 读取文件内容
    val lineRDD: RDD[String] = sparkContext.textFile(file)

    val wordPairRDD: RDD[String] = lineRDD.flatMap(line => {
      val words: Array[String] =
        line.split(" ").filter(word => !word.equals(""))
      for (i <- 0 until words.length - 1) yield words(i) + "-" + words(i + 1)
    })

    // 定义特殊字符列表
    val spclChars: List[String] = List("&", "|", "#", "^", "@")

    // 定义判定函数f
    def f(s: String): Boolean = {
      val words: Array[String] = s.split("-")
      return !words.exists(spclChars.contains)
    }

    val cleanedPairRDD: RDD[String] = wordPairRDD.filter(f)
    val kvRDD: RDD[(String, Int)] =
      cleanedPairRDD.map(wordPair => (wordPair, 1))
    val wordCounts: RDD[(String, Int)] = kvRDD.reduceByKey((x, y) => x + y)

    wordCounts
      .map { case (k, v) => (v, k) }
      .sortByKey(false)
      .take(5)
      .foreach(println)
  } catch {
    case ex: Exception => {
      ex.printStackTrace() // 打印到标准err
      System.err.println("exception===>: ...") // 打印到标准err
    }
  }
}
