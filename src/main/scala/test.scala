/**
  * Created by cycy on 2018/1/26.
  */
import java.lang.Exception
import java.util.Scanner

import org.apache.commons.collections.functors.ExceptionFactory
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import utils.Graspan_OP

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
    var newedges_interval:Int=40000000

    var openBloomFilter:Boolean=false
    var edges_totalnum:Int=1
    var error_rate:Double=0.1

    var htable_name:String="edges"
    var queryHBase_interval:Int=2
    var HRegion_splitnum:Int=36

    var is_complete_loop:Boolean=false
    var max_complete_loop_turn:Int=5
    var max_delta:Int=10000

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
        case "newedges_interval"=>newedges_interval=argvalue.toInt

        case "openBloomFilter"=>openBloomFilter=argvalue.toBoolean
        case "edges_totalnum"=>edges_totalnum=argvalue.toInt
        case "error_rate"=>error_rate=argvalue.toDouble

        case "htable_name"=>htable_name=argvalue
        case "queryHBase_interval"=>queryHBase_interval=argvalue.toInt
        case "HRegion_splitnum"=>HRegion_splitnum=argvalue.toInt

        case "is_complete_loop"=>is_complete_loop=argvalue.toBoolean
        case "max_complete_loop_turn"=>max_complete_loop_turn=argvalue.toInt
        case "max_delta"=>max_delta=argvalue.toInt

        case _ => {}
      }
    }

    /**
      * 输出参数设置
      */



    /**
      * Spark 设置
      */
    val conf = new SparkConf().set("spark.kryoserializer.buffer.max", "1024")
    if (islocal) {
      //test location can be adjusted or not
      conf.setAppName("Graspan")
      System.setProperty("hadoop.home.dir", "F:/hadoop-2.6.0/")
      conf.setMaster("local")
    }
    val sc = new SparkContext(conf)
    try {
      /**
        * Grammar相关设置
        */
      val grammar_origin = sc.textFile(input_grammar).map(s => s.split("\\s+")).collect().toList
      val (symbol_Map, symbol_num, symbol_num_bitsize, loop, directadd, grammar) = Graspan_OP.processGrammar(grammar_origin,
        input_grammar)
      println("------------Grammar INFO--------------------------------------------")
      println("input grammar:      \t") + input_grammar
      println("symbol_num_bitsize: \t" + symbol_num_bitsize)
      println("symbol_Map:         \t")
      symbol_Map.foreach(s => println("                    \t" + s._2 + "\t->\t" + s._1))
      println
      println("loop:               \t")
      loop.foreach(s => println("                    \t" + s))
      println
      println("directadd:          \t")
      directadd.foreach(s => println("                    \t" + s._1 + "\t->\t" + s._2))
      println
      println("grammar_clean:      \t")
      grammar.foreach(s => println("                    \t" + s._1._1 + "\t+\t" + s._1._2 + "\t->\t" + s._2))
      println("---------------------------------------------------------------------")
      println

      /**
        * Graph相关设置
        */
      val (graph, nodes_num_bitsize, nodes_totalnum) = Graspan_OP.processGraph(sc, input_graph, input_grammar, symbol_Map, loop,
        directadd, defaultpar)

      println("------------Graph INFO--------------------------------------------")
      println("input graph:        \t" + input_graph)
      println("processed edges:    \t" + graph.count())
      println("nodes_totoalnum:    \t" + nodes_totalnum)
      println("nodes_num_bitsize:  \t" + nodes_num_bitsize)
      println("------------------------------------------------------------------")
      println

      //first test
      {
        val partitioner = new HashPartitioner(64)
        val graph_rep1 = graph.map(s => (s._1, s)).partitionBy(partitioner)
        val graph_rep2 = graph.map(s => (s._1, s)).partitionBy(partitioner)
        val cog = (graph_rep1 join graph_rep2)
          .map(s => (s._1, s._2._1))
        println(cog.collect())
        val scan = new Scanner(System.in)
        scan.next()
      }

      //second test
      {
        val graph_rep1 = graph.map(s => (s._1, s)).repartition(64)
        val graph_rep2 = (graph ++ graph ++ graph ++ graph).map(s => (s._1, s)).repartition(128)
        val cog = (graph_rep2 join graph_rep1)
          .map(s => (s._1, s._2._1))
        println(cog.collect())
        val scan = new Scanner(System.in)
        scan.next()
      }

      sc.stop()
    }
      catch{
        case e:Exception => {
          println("sc stop")
          sc.stop()
        }
        case _=>{
          println("sc stop")
          sc.stop()
        }
      }
    finally{
      println("sc stop")
      sc.stop()
    }
  }
}
