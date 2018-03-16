/**
  * Created by cycy on 2018/2/3.
  */

import java.util
import java.util.Scanner
import java.util.concurrent.Executors

import org.apache.hadoop.conf.Configuration
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client.{HBaseAdmin, HTable}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.storage.StorageLevel
import utils.{Graspan_OP, HBase_OP, Para, deleteDir,UDFPartitioner}

object Graspan_UDFP extends Para{

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
    var Max_Par_Size:Int=50000000
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
        case "defaultpar"=>defaultpar=argvalue.toInt
        case "Max_Par_Size"=>Max_Par_Size=argvalue.toInt
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
      println("------------Spark and HBase settings--------------------------------")
      println("spark.driver.memory:  \t" + conf.get("spark.driver.memory"))
      println("spark.executor.memory: \t" + conf.get("spark.executor.memory"))
      println("spark.executor.cores: \t" + conf.get("spark.executor.cores"))
      println("default partition num: \t" + defaultpar)
      println("samll partition num:  \t" + smallpar)
      println("queryHBase_interval:  \t" + queryHBase_interval)
      println("HRegion_splitnum:     \t" + HRegion_splitnum)
      println("--------------------------------------------------------------------")
      println
      /**
        * Grammar相关设置
        */
      val grammar_origin = sc.textFile(input_grammar).map(s => s.split("\\s+")).collect().toList
      val (symbol_Map, symbol_num, symbol_num_bitsize, loop, directadd, grammar) = Graspan_OP.processGrammar(grammar_origin,
        input_grammar)
      println("------------Grammar INFO--------------------------------------------")
      println("input grammar:      \t" + input_grammar.split("/").last)
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
      val (graph, nodes_num_bitsize, nodes_totalnum) = Graspan_OP.processGraph(sc, input_graph, 0,0,input_grammar,
        symbol_Map, loop,
        directadd, defaultpar)

      println("------------Graph INFO--------------------------------------------")
      println("input graph:        \t" + input_graph.split("/").last)
      println("processed edges:    \t" + graph.count())
      println("nodes_totoalnum:    \t" + nodes_totalnum)
      println("nodes_num_bitsize:  \t" + nodes_num_bitsize)
      println("------------------------------------------------------------------")
      println
      val htable_nodes_interval: Int = nodes_totalnum / HRegion_splitnum + 1
      val (htable_split_Map, default_split) = HBase_OP.createHBase_Table(htable_name, HRegion_splitnum)

      /**
        * 原边集存入Hbase
        */
      //    println("graph Partitions: "+graph.partitions.length)
      deleteDir.deletedir(islocal, master, hbase_output)
      HBase_OP.updateHbase(graph, nodes_num_bitsize, symbol_num_bitsize, htable_name, hbase_output,
        htable_split_Map, htable_nodes_interval, default_split)

      /**
        * 开始迭代
        */
      deleteDir.deletedir(islocal, master, output)
      var compute_par_num = defaultpar
      var compute_Partitioner = new HashPartitioner(compute_par_num)
      var old_par_num = defaultpar
      var old_Partitioner = new HashPartitioner(old_par_num)
      var oldedges: RDD[(VertexId, List[((VertexId, VertexId), EdgeLabel)])] = sc.parallelize(List())
      var newedges: RDD[(VertexId, List[((VertexId, VertexId), EdgeLabel)])] = graph.flatMap(s => List((s._1, ((s._1, s
        ._2), s._3)), (s._2, ((s._1, s._2), s._3)))).groupByKey().mapValues(s => s.toList).partitionBy(old_Partitioner)
      var step = 0
      var continue: Boolean = !newedges.isEmpty()
      var newnum: Long = graph.count()
      while (continue) {
        t0 = System.nanoTime(): Double
        step += 1
        println("\n************During step " + step + "************")
        val tmp_old = oldedges
        val tmp_new = newedges
        val last_compute_par_num = compute_par_num

        if (newnum > 100000) {
          compute_par_num = (newnum / newedges_interval + 1).toInt * defaultpar
          println("large way: \t" + compute_par_num)
        }
        else {
          compute_par_num = ((newnum / 1000) + 1).toInt
          println("small way: \t" + compute_par_num)
        }
        if (last_compute_par_num != compute_par_num) {
          compute_Partitioner = new HashPartitioner(compute_par_num)
        }
        /**
          * 计算
          */
        val new_edges_str ={
          val tmp_rdd=(oldedges rightOuterJoin newedges).mapValues(s =>(s._1.getOrElse(List()),s._2))
          val t0_par=System.nanoTime():Double
          val node_size=tmp_rdd.mapValues(s=>s._2.length.toLong*(s._1.length.toLong+s._2.length.toLong/2))
          val node_par=Graspan_OP.arrangePartition(node_size,Max_Par_Size,defaultpar)
          println("node par distribution")
          println(node_par.mapValues(s=>s/10000000).map(s=>s.swap).groupByKey().mapValues(s=>s.size).map(s=>"Size" +
            " > " +s._1+"0000000, node num "+s._2).collect().mkString("\n"))
          val all_par_num=node_par.map(s=>s._2).distinct.count()
          println("compute par num:             \t"+all_par_num)
          println("compute par take time:       \t"+((System.nanoTime()-t0_par)/1000000000.0).formatted("%.3f")+" secs")
          (tmp_rdd join node_par).map(s=>(s._2._2,(s._1,s._2._1))).partitionBy(new UDFPartitioner(all_par_num.toInt))
          //        .partitionBy(compute_Partitioner)
          .mapPartitionsWithIndex((index, s) => Graspan_OP.computeInPartition_completely_UDFP(step, index, s, grammar,
          htable_name, nodes_num_bitsize,
          symbol_num_bitsize, directadd, is_complete_loop, max_complete_loop_turn, max_delta, htable_split_Map,
          htable_nodes_interval, queryHBase_interval, default_split)).persist(StorageLevel.MEMORY_AND_DISK)
        }
        /**
          * 记录各分区情况
          */
        val par_INFO = new_edges_str.map(s=>s._2)
        deleteDir.deletedir(islocal, master, output + "par_INFO/step" + step)
        par_INFO.repartition(1).saveAsTextFile(output + "par_INFO/step" + step)
        val coarest_num=new_edges_str.map(s=>s._3).sum
        println("coarest_num:                   \t"+coarest_num.toLong)
        /**
          * 新边去重
          */
        val newedges_dup = new_edges_str.flatMap(s => s._1)
        println("newedges_dup:                  \t"+newedges_dup.count())
        val t1_compute=System.nanoTime():Double
        println("compute take time:             \t" + ((t1_compute-t0)/1000000000.0).formatted("%.3f")+" secs")

        val newedges_removedup = newedges_dup.distinct.persist(StorageLevel.MEMORY_AND_DISK)
        newnum = newedges_removedup.count()
        println("newedges:                      \t" + newnum)
        println("distinct take time:            \t" + ((System.nanoTime()-t1_compute)/1000000000.0).formatted("%.3f")+" secs")
        new_edges_str.unpersist()

        /**
          * 更新旧边和新边
          */
        oldedges = (oldedges cogroup newedges)
          .mapValues(s => s._1.headOption.getOrElse(List()) ++ s._2.headOption.getOrElse(List()))
          .persist(StorageLevel.MEMORY_AND_DISK)
        //      .partitionBy(old_Partitioner).persist(StorageLevel.MEMORY_AND_DISK)
        tmp_old.unpersist()
        tmp_new.unpersist()
        newedges = newedges_removedup.flatMap(s => List((s._1, ((s._1, s._2), s._3)), (s._2, ((s._1, s._2), s._3))))
          .groupByKey().mapValues(s => s.toList)
          .partitionBy(old_Partitioner).cache()
        //      println("oldedges:           \t"+oldedges.map(s=>s._2.length).sum().toLong/2)

        /**
          * Update HBase
          */
        val t0_hb = System.nanoTime(): Double
        deleteDir.deletedir(islocal, master, hbase_output)
        HBase_OP.updateHbase(newedges_removedup, nodes_num_bitsize, symbol_num_bitsize, htable_name, hbase_output,
          htable_split_Map, htable_nodes_interval, default_split)
        newedges_removedup.unpersist()
        val t1_hb = System.nanoTime(): Double
        println("update Hbase take time:        \t" + ((t1_hb - t0_hb) / 1000000000.0).formatted("%.3f") + " sec")
        t1 = System.nanoTime(): Double
        println("*step: step " + step + " take time: \t " + ((t1 - t0) / 1000000000.0).formatted("%.3f") + " sec")
        println
        continue = newnum != 0
      }

      println("final edges count:             \t" + oldedges.map(s => s._2.length).sum().toLong / 2)
      //    h_admin.close()
      //    h_table.close()
      val scan = new Scanner(System.in)
      sc.stop()
    }
    catch{
      case e:Exception => {
        println("sc stop")
        sc.stop()
      }
    }
    finally {
      println("sc stop")
      sc.stop()
    }
  }

}



