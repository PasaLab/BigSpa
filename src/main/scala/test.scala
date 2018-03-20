/**
  * Created by cycy on 2018/1/26.
  */
import java.lang.Exception
import java.util.Scanner

import org.apache.commons.collections.functors.ExceptionFactory
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import utils.{Graspan_OP, deleteDir}

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

object test extends utils.Para{
  def main(args: Array[String]): Unit = {
    var t0=System.nanoTime():Double
    var t1=System.nanoTime():Double
    var islocal: Boolean = true
    var master: String = "local"

    var input_grammar: String = "data/GrammarFiles/test_grammar"
    var input_graph:String="data/InputGraph/test_graph"
    var output: String = "data/result/" //除去ip地址
    var hbase_output:String="data/result/hbase/hbhfile/"
    var defaultpar:Int=352
    var smallpar:Int=64

    for (arg <- args) {
      val argname = arg.split(",")(0)
      val argvalue = arg.split(",")(1)
      argname match {
        case "islocal" => islocal = argvalue.toBoolean
        case "master" => master = argvalue

        case "input_grammar" => input_grammar = argvalue
        case "input_graph"=>input_graph=argvalue
        case "output" => output = argvalue
        case "hbase_output"=>hbase_output=argvalue
        case "smallpar"=>smallpar=argvalue.toInt

        case _ => {}
      }
    }

    /**
      * 输出参数设置
      */

    /**
      * Spark 设置
      */
//    val conf = new SparkConf().set("spark.kryoserializer.buffer.max", "1024")
//    if (islocal) {
//      //test location can be adjusted or not
//      conf.setAppName("Graspan")
//      System.setProperty("hadoop.home.dir", "F:/hadoop-2.6.0/")
//      conf.setMaster("local")
//    }
//    val sc = new SparkContext(conf)
//    sc.setLogLevel("ERROR")

val scan=new Scanner(System.in)
val a=scan.nextLine()
    val b=a.split("\\s+").map(_.toLong).zipWithIndex
    val b_0=b.filter(x=>x._2%3==0).map(x=>x._1)
    val b_1=b.filter(x=>x._2%3==1).map(x=>x._1)
    val number=(b_0++b_1).distinct.sorted
    println(number.length)
    println(number(0))
    println(number.last)




  }
}
