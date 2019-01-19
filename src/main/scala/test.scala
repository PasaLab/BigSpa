/**
  * Created by cycy on 2018/1/26.
  */
import java.lang.Exception
import java.util.Scanner
import utils.BIgSpa_OP._
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import utils.{BIgSpa_OP, BigSpa_OP_java, HBase_OP, deleteDir}

import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import scala.util.Random
import it.unimi.dsi.fastutil.ints._
import it.unimi.dsi.fastutil.longs.{LongArrayList, LongComparator, LongOpenHashSet}
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.util.Bytes

object test {
  def main(args: Array[String]): Unit = {
    var islocal: Boolean = true
    var master: String = "local"

//    var input_graph: String = "sharex/sharex_final_output"
  var input_graph: String = "E:\\BigSpa\\azure-powershell\\final_output.txt"
    var output: String = "E:\\BigSpa\\azure-powershell\\e_n\\"
    for (arg <- args) {
      val argname = arg.split(",")(0)
      val argvalue = arg.split(",")(1)
      println("arg: " + arg)
      argname match {
        case "islocal" => islocal = argvalue.toBoolean
        case "master" => master = argvalue
        case "input_graph" => input_graph = argvalue
        case "output" => output = argvalue
        case _ => {}
      }
    }
      val conf = new SparkConf()
      if (islocal) {
        //test location can be adjusted or not
        conf.setAppName("Graspan")
        System.setProperty("hadoop.home.dir", "F:/hadoop-2.6.0/")
        conf.setMaster("local")
      }
      val sc = new SparkContext(conf)
      deleteDir.deletedir(islocal,master,output)
      processDF(sc, input_graph, output, 384)
    }
}
/*
class Long_Comparator extends LongComparator {
  def compare(a1: Long, a2: Long): Int = if (a1 < a2) -1
  else if (a1 > a2) 1
  else 0
}*/
