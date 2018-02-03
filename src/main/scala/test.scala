/**
  * Created by cycy on 2018/1/26.
  */
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import utils.{Graspan_OP, HBase_OP, deleteDir}

import scala.collection.JavaConversions._
object test {
  def main(args: Array[String]): Unit = {
    var islocal: Boolean = true
    var master: String = "local"
//
    var input_grammar: String = "data/GrammarFiles/test_grammar"
    var input_graph:String="data/InputGraph/test_graph"
    var output: String = "data/result/" //除去ip地址
//    var hbase_output:String="data/result/hbase/hbhfile/"
//    var par: Int = 96
//
//    for (arg <- args) {
//      val argname = arg.split(",")(0)
//      val argvalue = arg.split(",")(1)
//      argname match {
//        case "islocal" => islocal = argvalue.toBoolean
//        case "master" => master = argvalue
//
//        case "input_grammar" => input_grammar = argvalue
//        case "input_graph"=>input_graph=argvalue
//        case "output" => output = argvalue
//        case "hbase_output"=>hbase_output=argvalue
//        case "par" => par = argvalue.toInt
//
//        case _ => {}
//      }
//    }
    val conf = new SparkConf().set("spark.kryoserializer.buffer.max", "1024")
    if (islocal) {
      //test location can be adjusted or not
      conf.setAppName("Graspan")
      System.setProperty("hadoop.home.dir", "F:/hadoop-2.6.0/")
      conf.setMaster("local")
    }
    val sc = new SparkContext(conf)
//
//    var Linux_Path:String=""
//    if(input_graph.contains("Linux")) {
//      if (input_grammar.contains("pointsto")) {
//        Linux_Path = output + "Linux_pointsto_data/"
//        deleteDir.deletedir(islocal, master, Linux_Path)
//      }
//      else {
//        Linux_Path = output + "Linux_dataflow_data/"
//        deleteDir.deletedir(islocal, master, Linux_Path)
//      }
//      Graspan_OP.processLinux(sc,input_graph,input_grammar,Linux_Path)
//    }
    val hashway=new HashPartitioner(2)
    val rdd1=sc.parallelize(List((1,1),(1,2),(2,3),(3,4)),2).groupByKey().map(s=>(s._1,s._2.toList))
    val rdd2=sc.parallelize(List((1,1),(2,2),(2,3),(4,4)),4).groupByKey().map(s=>(s._1,s._2.toList))
    val rddjoin=(rdd1 leftOuterJoin  rdd2).map(s=>(s._1,s._2._1 ++ s._2._2.headOption.getOrElse(List())))
    println(rddjoin.partitions.length)
  }
}
