
/**
  * Created by cycy on 2018/1/18.
  * Grammar Format: A B C.---------->     A<-BC
  * InputGraph Format: src,dst,Label
  */

import java.util
import java.util.concurrent.Executors

import org.apache.hadoop.conf.Configuration
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client.{HBaseAdmin, HTable}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import utils.{Graspan_OP, HBase_OP, Para, deleteDir}

object Graspan_improve extends Para{

  def main(args: Array[String]): Unit = {
    var t0=System.nanoTime():Double
    var t1=System.nanoTime():Double
    var islocal: Boolean = true
    var master: String = "local"

    var input_grammar: String = "data/GrammarFiles/test_grammar"
    var input_graph:String="data/InputGraph/test_graph"
    var output: String = "data/result/" //除去ip地址
    var hbase_output:String="data/result/hbase/hbhfile"
    var par: Int = 96
    var smallpar:Int=16

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
        case "par" => par = argvalue.toInt
        case "smallpar"=>smallpar=argvalue.toInt

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
    println("------------Spark and HBase settings--------------------------------")
    println("spark.driver.memory:  \t"+conf.get("spark.driver.memory"))
    println("spark.executor.memory: \t"+conf.get("spark.executor.memory"))
    println("spark.executor.cores: \t"+conf.get("spark.executor.cores"))
    println("partition num:        \t"+par)
    println("queryHBase_interval:  \t"+queryHBase_interval)
    println("HRegion_splitnum:     \t"+HRegion_splitnum)
    println("--------------------------------------------------------------------")
    println
    /**
      * Grammar相关设置
      */
    val grammar_origin=sc.textFile(input_grammar).map(s=>s.split("\\s+")).collect().toList
    val (symbol_Map,symbol_num,symbol_num_bitsize,loop,directadd,grammar)=Graspan_OP.processGrammar(grammar_origin,
      input_grammar)
    println("------------Grammar INFO--------------------------------------------")
    println("symbol_num_bitsize: \t"+symbol_num_bitsize)
    println("symbol_Map:         \t")
    symbol_Map.foreach(s=>println("                    \t"+s._2+"\t->\t"+s._1))
    println
    println("loop:               \t")
    loop.foreach(s=>println("                    \t"+s))
    println
    println("directadd:          \t")
    directadd.foreach(s=>println("                    \t"+s._1+"\t->\t"+s._2))
    println
    println("grammar_clean:      \t")
    grammar.foreach(s=>println("                    \t"+s._1._1+"\t+\t"+s._1._2+"\t->\t"+s._2))
    println("---------------------------------------------------------------------")
    println

    /**
      * Graph相关设置
      */
    val graph_origin=sc.textFile(input_graph,par).filter(!_.trim.equals("")).map(s=>{
      val str=s.split("\\s+")
      (str(0).toInt,str(1).toInt,str(2))
    })
    if(graph_origin.isEmpty()){
      println("input graph is empty")
      System.exit(0)
    }
    val(graph,nodes_num_bitsize,nodes_totalnum)=Graspan_OP.processGraph(graph_origin,input_grammar,symbol_Map,loop,
      directadd,par)
    println("------------Graph INFO--------------------------------------------")
    println("graph_origin edges: \t"+graph_origin.count())
    println("processed edges:    \t"+graph.count())
    println("nodes_totoalnum:    \t"+nodes_totalnum)
    println("nodes_num_bitsize:  \t"+nodes_num_bitsize)
    println("------------------------------------------------------------------")
    println
    val htable_nodes_interval:Int=nodes_totalnum/HRegion_splitnum+1
    val (htable_split_Map,default_split)=HBase_OP.createHBase_Table(htable_name,HRegion_splitnum)

    /**
      * 原边集存入Hbase
      */
    //    println("graph Partitions: "+graph.partitions.length)
    deleteDir.deletedir(islocal,master,hbase_output)
    HBase_OP.updateHbase(graph,nodes_num_bitsize,symbol_num_bitsize,htable_name,hbase_output,
      htable_split_Map,htable_nodes_interval,default_split)

    /**
      * 开始迭代
      */
    deleteDir.deletedir(islocal,master,output)
    var oldedges:RDD[(VertexId,VertexId,EdgeLabel,Boolean)]=sc.parallelize(List())
    var newedges:RDD[(VertexId,VertexId,EdgeLabel,Boolean)]=graph.map(s=>(s._1,s._2,s._3,true))
    var step=0
    var continue:Boolean= !newedges.isEmpty()
    var newnum:Long=newedges.count()
    while(continue){
      t0=System.nanoTime():Double
      step+=1
      println("\n************During step "+step+"************")
      val t0_compute=System.nanoTime():Double
      val newedges_dup_str={
        if(newnum<100000){
          println("alter way")
          val candiSet_front=newedges.map(s=>s._1).collect().toSet
          val candiSet_back=newedges.map(s=>s._2).collect().toSet
          (oldedges.filter(s=>candiSet_front.contains(s._2)||candiSet_back.contains(s._1)) ++ newedges)
            .flatMap(s=>List((s._1,((s._1,s._2),s._3,s._4)),(s._2,
              ((s._1,s._2),s._3,s._4))))
            .groupByKey()
            .map(s=>(s._1,s._2.toList))
            .repartition(par)
            .mapPartitionsWithIndex((index,s)=>Graspan_OP.computeInPartition_completely(step,index,s,grammar,
              htable_name,
              nodes_num_bitsize,symbol_num_bitsize,directadd,
              is_complete_loop,max_complete_loop_turn,max_delta,
              htable_split_Map,htable_nodes_interval,queryHBase_interval,default_split)).cache()
        }
        else
          (oldedges ++ newedges).flatMap(s=>List((s._1,((s._1,s._2),s._3,s._4)),(s._2,
            ((s._1,s._2),s._3,s._4))))
            .groupByKey()
            .map(s=>(s._1,s._2.toList))
            .repartition(par)
            .mapPartitionsWithIndex((index,s)=>Graspan_OP.computeInPartition_completely(step,index,s,grammar,
              htable_name,
              nodes_num_bitsize,symbol_num_bitsize,directadd,
              is_complete_loop,max_complete_loop_turn,max_delta,
              htable_split_Map,htable_nodes_interval,queryHBase_interval,default_split)).cache()
      }

      val (newedges_dup,partition_info)=(newedges_dup_str.flatMap(s=>s._1),newedges_dup_str.map(s=>s._2))

//      println("new_edges_bf count inPartition:")
//      deleteDir.deletedir(islocal,master,output+"step"+step+"/ParINFO/")
//      partition_info.saveAsTextFile(output+"step"+step+"/ParINFO/")
      //      println(partition_info.collect().mkString("\n"))
      val t1_compute=System.nanoTime():Double
      println("clousure compute take time:    \t"+((t1_compute-t0_compute) / 1000000000.0).formatted("%.3f") + " sec")

      /**
        * 获得各分区内经过Bloom Filter和Hbase过滤之后留下的新边
        * 再汇总distinct
        */
      val t0_distinct=System.nanoTime():Double
//      println("newedges_dup:                  \t"+newedges_dup.count())
      val newedges_removedup=newedges_dup.distinct()
//      deleteDir.deletedir(islocal,master,output+"step"+step+"/new/")
//      newedges_dup.map(s=>(s._1+"\t"+s._2+"\t"+s._3)).saveAsTextFile(output+"step"+step+"/new/")
      println("newedges_removedup:            \t"+newedges_removedup.count())
      newnum=newedges_removedup.count()
      //      println(newedges_removedup.collect().mkString("\n"))
      val t1_distinct=System.nanoTime():Double
      println("distinct take time:           \t "+((t1_distinct-t0_distinct) / 1000000000.0).formatted("%.3f") + " " +
        "sec")

      /**
        * Hbase更新
        */
      val t0_hb=System.nanoTime():Double
      deleteDir.deletedir(islocal,master,hbase_output)
      HBase_OP.updateHbase(newedges_removedup,nodes_num_bitsize,symbol_num_bitsize,htable_name,hbase_output,
        htable_split_Map,htable_nodes_interval,default_split)
      val t1_hb=System.nanoTime():Double
      println("update Hbase take time:        \t"+((t1_hb-t0_hb) / 1000000000.0).formatted("%.3f") + " sec")

      val tmp_old = oldedges
      val tmp_new = newedges
      oldedges=(oldedges ++ newedges.map(s=>(s._1,s._2,s._3,false))).repartition(par).cache()
//      deleteDir.deletedir(islocal,master,output+"step"+step+"/old/")
//      oldedges.map(s=>(s._1+"\t"+s._2+"\t"+s._3)).saveAsTextFile(output+"step"+step+"/old/")
      newedges=newedges_removedup.map(s=>(s._1,s._2,s._3,true)).cache()
      continue= !(newedges.isEmpty())
      t1=System.nanoTime():Double
      tmp_old.unpersist()
      tmp_new.unpersist()
//      println("all edges sum to:              \t"+oldedges.count())
      println("*step: step "+step+" take time: \t "+((t1 - t0) / 1000000000.0).formatted("%.3f") + " sec")
      println
    }

    println("final edges count:             \t"+oldedges.count())
    //    h_admin.close()
    //    h_table.close()
  }

}
