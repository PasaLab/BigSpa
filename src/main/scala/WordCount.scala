import java.util.Scanner

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
  * Created by cycy on 2018/7/23.
  */
object WordCount {
  def main(args: Array[String]) {

    val input="data/a letter to daughter"
    val conf = new SparkConf()//.set("spark.kryoserializer.buffer.max", "2048")
    if (true) {
      //test location can be adjusted or not
      conf.setAppName("WordCount")
      System.setProperty("hadoop.home.dir", "F:/hadoop-2.6.0/")
      conf.setMaster("local")
    }
    val sc = new SparkContext(conf)
    val line = sc.textFile(input).cache()

    println("出现次数最多的前10个单词：")
    line.flatMap(_.split("\\s+")).map((_, 1)).reduceByKey(_+_).sortBy(_._2,false).collect().slice(0,10).foreach(println)
    println("出现次数相同的单词，降序排列：")
    line.flatMap(_.split("\\s+")).map((_, 1)).reduceByKey(_+_)
      .map(x=>(x._2,x._1)).groupByKey().sortByKey(false).map(s=>s._1+": "+s._2.mkString(",")).collect().foreach(println)


    val line2=sc.textFile("H:\\WangleiCSV\\x_train.csv").count()
    val scan=new Scanner(System.in)
    scan.next()
    sc.stop()
  }
}
