//package ONLINE
//
//import java.util
//
//import it.unimi.dsi.fastutil.longs.LongOpenHashSet
//import org.apache.spark.{SparkConf, SparkContext}
//import utils.{BIgSpa_OP, Param_pt}
//
//import scala.collection.mutable.ArrayBuffer
//
///**
//  * Created by cycy on 2019/1/19.
//  */
//object DO_20190119 {
//  def main(args: Array[String]): Unit = {
//    val input_graph:String=""
//    val input_grammar:String=""
//    val input_cluster:String=""
//
//    /**
//      * 1 创建Spark环境
//      */
//    val conf = new SparkConf()
//    if (Param_pt.islocal) {
//      //test location can be adjusted or not
//      conf.setAppName("Graspan")
//      System.setProperty("hadoop.home.dir", "F:/hadoop-2.6.0/")
//      conf.setMaster("local")
//    }
//    val sc = new SparkContext(conf)
//    sc.setCheckpointDir(Param_pt.checkpoint_output)
//
//    /**
//      * 2 合并cluster和edge
//      */
//    /**
//      * Grammar相关设置
//      */
//    val grammar_origin = sc.textFile(Param_pt.input_grammar).filter(s=> !s.trim.equals("")).map(s => s.split("\\s+").map(_
//      .trim))
//      .collect().toList
//    val (symbol_Map, symbol_num, symbol_num_bitsize, loop, directadd, grammar) = BIgSpa_OP.processGrammar(grammar_origin,
//      Param_pt.input_grammar)
//    println("------------Grammar INFO--------------------------------------------")
//    println("input grammar:      \t" + Param_pt.input_grammar.split("/").last)
//    println("symbol_num:         \t" + symbol_num)
//    println("symbol_num_bitsize: \t" + symbol_num_bitsize)
//    println("symbol_Map:         \t")
//    symbol_Map.foreach(s => println("                    \t" + s._2 + "\t->\t" + s._1))
//    println
//    println("loop:               \t")
//    loop.foreach(s => println("                    \t" + s))
//    println
//    println("directadd:          \t")
//    directadd.foreach(s => println("                    \t" + s._1 + "\t->\t" + s._2))
//    println
//    println("grammar_clean:      \t")
//    grammar.foreach(s => println("                    \t" + s(0) + "\t+\t" + s(1) + "\t->\t" + s(2)))
//    println("---------------------------------------------------------------------")
//    println
//
//    val (graph, nodes_num_bitsize, nodes_totalnum) = BIgSpa_OP.processGraph(sc, Param_pt.input_graph, Param_pt.file_index_f,
//      Param_pt.file_index_b,
//      Param_pt.input_grammar,
//      symbol_Map, loop,
//      directadd, Param_pt.defaultpar)
//
//    println("------------Graph INFO--------------------------------------------")
//    println("input graph:        \t" + Param_pt.input_graph.split("/").last)
//    println("processed edges:    \t" + graph.count())
//    println("nodes_totoalnum:    \t" + nodes_totalnum)
//    println("nodes_num_bitsize:  \t" + nodes_num_bitsize)
//    println("------------------------------------------------------------------")
//    println
//
//    val v_adj=graph.flatMap(e=>{
//      Array((e(0),e),(e(1),e))
//    }).groupByKey()
//
//    val v_cluster=sc.textFile(input_cluster).map(line=>{
//      val tokens=line.split("\\s+")
//      (tokens(0).toInt,tokens(1).toInt)
//    })
//
//
//    val cluster_v_adj=(v_adj join v_cluster).map(s=>(s._2._2,(s._1,s._2._1))).groupByKey()
//
//    val cluster_transitive_closure1=cluster_v_adj.map(s=>(s._1,processCluster(s._2,grammar,symbol_num)))
//
//
//  }
//
//  def processCluster(v_info:Iterable[(Int,Iterable[Array[Int]])],grammar:Array[Array[Int]],symbol_num:Int)
//  :(Iterable[(Int, Array[Array[List[Int]]])],List[Long])={
//    val old_f=0
//    val old_b=1
//    val new_f=2
//    val new_b=3
//    var v_adj:Map[Int,Array[Array[ArrayBuffer[Int]]]]=v_info.toArray.map(s=>(s._1,{
//      val flag=s._1
//      val es=s._2.toArray
//      val adj=new Array[Array[ArrayBuffer[Int]]](symbol_num)
//      for(i <- 0 to symbol_num){
//        adj(i)=new Array[ArrayBuffer[Int]](4)
//      }
//      for(e <- es){
//        if(e(0)==flag){
//          adj(e(2))(new_f).append(e(1))
//        }
//        if(e(1)==flag){
//          adj(e(2))(new_b).append(e(0))
//        }
//      }
//      adj
//    })).toMap
//    val edge_base=new LongOpenHashSet()
//    val hub_edge_base=ArrayBuffer[Long]
//    var continue=true
//    while(continue){
//      val new_edges_set = new LongOpenHashSet()
//      new_edges_set.removeAll(edge_base)
//      for(v <-v_adj){
//
//      }
//      new_edges_set.removeAll(edge_base)
//      edge_base.addAll(new_edges_set)
//      if(new_edges_set.size()>0) continue=false
//    }
//  }
//}
//
