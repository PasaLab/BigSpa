/**
  * Created by cycy on 2018/1/26.
  */
import java.lang.Exception
import java.util.Scanner

import org.apache.commons.collections.functors.ExceptionFactory
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import utils.{Graspan_OP, Graspan_OP_java, HBase_OP, deleteDir}

import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import scala.util.Random
import it.unimi.dsi.fastutil.ints._
import it.unimi.dsi.fastutil.longs.{LongArrayList, LongComparator, LongOpenHashSet}
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.util.Bytes
object test extends utils.Para {
  def main(args: Array[String]): Unit = {
    var t0 = System.nanoTime(): Double
    var t1 = System.nanoTime(): Double
    var islocal: Boolean = true
    var master: String = "local"

    var input_grammar: String = "data/GrammarFiles/test_grammar"
    var input_graph: String = "H:\\Graspan资料\\Graspan数据和源代码\\Apache_Httpd_2.2.18_Points-to\\Apache_httpd_2.2.18_pointsto_graph"
    var output: String = "data/result/" //除去ip地址
    var hbase_output: String = "data/result/hbase/hbhfile/"
    var defaultpar: Int = 64
    var smallpar: Int = 64

    for (arg <- args) {
      val argname = arg.split(",")(0)
      val argvalue = arg.split(",")(1)
      argname match {
        case "islocal" => islocal = argvalue.toBoolean
        case "master" => master = argvalue

        case "input_grammar" => input_grammar = argvalue
        case "input_graph" => input_graph = argvalue
        case "output" => output = argvalue
        case "defaultpar" => defaultpar = argvalue.toInt
        case "hbase_output" => hbase_output = argvalue
        case "smallpar" => smallpar = argvalue.toInt

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

    val hBase_OP=new HBase_OP()
    val e=Array(0,1,2)
    println(hBase_OP.Array2String(e))
    val get =new Get(Bytes.toBytes(hBase_OP.Array2String(e))).setCheckExistenceOnly(true)


    val new_n = new LongArrayList()
    new_n.add(1)
    new_n.add(1)
    new_n.add(2)
    new_n.add(0)
    val long_tocheck=new LongArrayList()
    var new_n_array = {
      val tmp = new LongOpenHashSet(new_n)
      tmp.toLongArray.sorted
    }
    long_tocheck.addElements(long_tocheck.size(),new_n_array)
    println(new_n_array.mkString(","))
    println(long_tocheck.getLong(1))
  }

  }

class Long_Comparator extends LongComparator {
  def compare(a1: Long, a2: Long): Int = if (a1 < a2) -1
  else if (a1 > a2) 1
  else 0
}