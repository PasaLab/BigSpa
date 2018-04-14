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
import scala.io.Source
import scala.util.Random

object test extends utils.Para{
  def main(args: Array[String]): Unit = {
    var t0=System.nanoTime():Double
    var t1=System.nanoTime():Double
    var islocal: Boolean = true
    var master: String = "local"

    var input_grammar: String = "data/GrammarFiles/test_grammar"
    var input_graph:String="H:\\Graspan资料\\Graspan数据和源代码\\Apache_Httpd_2.2.18_Points-to\\Apache_httpd_2.2.18_pointsto_graph"
    var output: String = "data/result/" //除去ip地址
    var hbase_output:String="data/result/hbase/hbhfile/"
    var defaultpar:Int=64
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
        case "defaultpar"=>defaultpar=argvalue.toInt
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
//    val conf = new SparkConf()
//    if (islocal) {
//      //test location can be adjusted or not
//      conf.setAppName("Graspan")
//      System.setProperty("hadoop.home.dir", "F:/hadoop-2.6.0/")
//      conf.setMaster("local")
//    }
//    val sc = new SparkContext(conf)
//    sc.setLogLevel("ERROR")
//    deleteDir.deletedir(islocal,master,output)

    val a=Array(1,2,3)
    val b=new Array[Int](4)
    System.arraycopy(a,0,b,0,0)
    println(b.mkString(","))

    }
}
