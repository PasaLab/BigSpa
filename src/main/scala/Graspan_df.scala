/**
  * Created by cycy on 2018/3/22.
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
import utils._

import scala.collection.mutable.ArrayBuffer

object Graspan_df extends Para{

  def check_edge_RDD(edges:RDD[(Int,(Array[Int],Int))]):RDD[String]={
    edges.map(s=>{
      val len=s._2._1.length
      val index_f_end=s._2._2
      if(index_f_end<len && len%2==0 && index_f_end%2!=0 ) "OK"
      else "edges: "+len+","+index_f_end+
        s"  is 4 part index error: ${index_f_end<len}"+
        s" is fill array error: ${len%2==0 && index_f_end%2!=0}"
    })
  }
  def main(args: Array[String]): Unit = {
    val scan = new Scanner(System.in)

    var t0=System.nanoTime():Double
    var t1=System.nanoTime():Double
    var islocal: Boolean = true
    var master: String = "local"

    var input_e:String="data/test_graph"
    var input_n:String="data/test_graph"
    var output: String = "data/result/" //除去ip地址
    var hbase_output:String="data/result/hbase/hbhfile/"
    var checkpoint_output:String="data/checkpoint"
    var defaultpar:Int=352
    var clusterpar:Int=352
    var newnum_interval:Int=40000000
    var checkpoint_interval:Int=10
    var newedges_interval:Int=40000000

    var openBloomFilter:Boolean=false
    var edges_totalnum:Int=1
    var error_rate:Double=0.1

    var htable_name:String="edges"
    var queryHBase_interval:Int=50000
    var HRegion_splitnum:Int=36
    var Batch_QueryHbase:Boolean=true

    var is_complete_loop:Boolean=false
    var max_complete_loop_turn:Int=5
    var max_convergence_loop:Int=100

    var file_index_f:Int= -1
    var file_index_b:Int= -1

    var check_edge:Boolean=false
    var isSingleton:Boolean=false

    for (arg <- args) {
      val argname = arg.split(",")(0)
      val argvalue = arg.split(",")(1)
      argname match {
        case "islocal" => islocal = argvalue.toBoolean
        case "master" => master = argvalue

        case "input_e"=>input_e=argvalue
        case "input_n"=>input_n=argvalue
        case "output" => output = argvalue
        case "hbase_output"=>hbase_output=argvalue
        case "clusterpar"=>clusterpar=argvalue.toInt
        case "defaultpar"=>defaultpar=argvalue.toInt
        case "newedges_interval"=>newedges_interval=argvalue.toInt

        case "openBloomFilter"=>openBloomFilter=argvalue.toBoolean
        case "edges_totalnum"=>edges_totalnum=argvalue.toInt
        case "error_rate"=>error_rate=argvalue.toDouble

        case "htable_name"=>htable_name=argvalue
        case "queryHBase_interval"=>queryHBase_interval=argvalue.toInt
        case "HRegion_splitnum"=>HRegion_splitnum=argvalue.toInt
        case "Batch_QueryHbase"=>Batch_QueryHbase=argvalue.toBoolean

        case "is_complete_loop"=>is_complete_loop=argvalue.toBoolean
        case "max_complete_loop_turn"=>max_complete_loop_turn=argvalue.toInt
        case "max_convergence_loop"=>max_convergence_loop=argvalue.toInt

        case "newnum_interval"=>newnum_interval=argvalue.toInt
        case "checkpoint_interval"=>checkpoint_interval=argvalue.toInt
        case "checkpoint_output"=>checkpoint_output=argvalue
        case "file_index_f"=>file_index_f=argvalue.toInt
        case "file_index_b"=>file_index_b=argvalue.toInt

        case "check_edge"=>check_edge=argvalue.toBoolean
        case "isSingleton"=>isSingleton=argvalue.toBoolean
        case _ => {}
      }
    }


    /**
      * Spark 设置
      */
    val conf = new SparkConf()//.set("spark.kryoserializer.buffer.max", "2048")
    if (islocal) {
      //test location can be adjusted or not
      conf.setAppName("Graspan")
      System.setProperty("hadoop.home.dir", "F:/hadoop-2.6.0/")
      conf.setMaster("local")
    }
    val sc = new SparkContext(conf)
    sc.setCheckpointDir(checkpoint_output)
    //    try {
    println("------------Spark and HBase settings--------------------------------")
    println("spark.driver.memory:          \t" + conf.get("spark.driver.memory"))
    println("spark.executor.memory:        \t" + conf.get("spark.executor.memory"))
    println("spark.executor.cores:         \t" + conf.get("spark.executor.cores"))
    println("default partition num:        \t" + defaultpar)
    println("cluster partition num:        \t" + clusterpar)
    println("queryHBase_interval:          \t" + queryHBase_interval)
    println("HRegion_splitnum:             \t" + HRegion_splitnum)
    println("--------------------------------------------------------------------")
    println
    /**
      * Grammar相关设置
      */
    val (symbol_num, symbol_num_bitsize,symbol_Map) =(2,1,Map(("n",0),("e",1)))
    /**
      * Graph相关设置
      */

    val (e_str,n_str)={
      if(input_e.contains("Linux_dataflow_e")){
        println("getinput_EandN")
        Graspan_OP.getinput_EandN(input_e,input_n,file_index_f,file_index_b,master)
      }
      else{
        (input_e,input_n)
      }
    }


    val e=sc.textFile(e_str,defaultpar).flatMap(s=>{
      val flag=s.split(":")(0).toInt
      s.split(":")(1).split("\\s+").map(x=>Array(flag,x.toInt,1))
    })
    val n=sc.textFile(n_str,defaultpar).map(s=>s.split("\\s+").map(_.toInt))
    println("e counts : "+e.filter(s=>s(2)==1).count())
    println("n counts : "+n.filter(s=>s(2)==0).count())
    val nodes_totalnum=(e.flatMap(s=>Array(s(0),s(1))) ++ n.flatMap(s=>Array(s(0),s(1)))).distinct().count()
    val nodes_num_bitsize=HBase_OP.getIntBit(nodes_totalnum.toInt)
    println("------------Graph INFO--------------------------------------------")
    println("input graph:        \t" + input_e)
    println("processed e:        \t" + e.count())
    println("processed n:        \t" + n.count())
    println("nodes_totoalnum:    \t" + nodes_totalnum)
    println("nodes_num_bitsize:  \t" + nodes_num_bitsize)
    println("max_loop_turn:      \t" + max_complete_loop_turn)
    println("convergence_turn:   \t" +max_convergence_loop)
    println("------------------------------------------------------------------")
    println

    val (htable_split_Map, default_split) = HBase_OP.createHBase_Table(htable_name, HRegion_splitnum)

    /**
      * 原边集存入Hbase
      */
    //    println("graph Partitions: "+graph.partitions.length)
    deleteDir.deletedir(islocal, master, hbase_output)
    HBase_OP.updateHbase_java_flat(e, nodes_num_bitsize, symbol_num_bitsize, htable_name, hbase_output,
      htable_split_Map, HRegion_splitnum, default_split)
    deleteDir.deletedir(islocal, master, hbase_output)
    HBase_OP.updateHbase_java_flat(n, nodes_num_bitsize, symbol_num_bitsize, htable_name, hbase_output,
      htable_split_Map, HRegion_splitnum, default_split)
    //    scan.next()
    deleteDir.deletedir(islocal, master, output)

    /**
      * 得到所有e的边
      */
    val e_bc={
      val e_edges=e.map(s=>(s(0),s(1))).groupByKey().mapValues(_.toArray).sortBy(_._1).collect()
      if(isSingleton==false){
        sc.broadcast(e_edges)
      }
      else {
        //        Dataflow_e_formation.form_e(master,input_e_nomaster)
        sc.broadcast(Array[(Int,Array[Int])]())
      }
    }


    var n_edges=n.map(s=>(s(1),s(0))).groupByKey().mapValues(_.toArray)
      .partitionBy(new HashPartitioner(defaultpar))
      .persist(StorageLevel.MEMORY_ONLY_SER)
    //    oldedges.count()

    //    println("check oldedges")
    //    println(check_edge_RDD(oldedges).filter(s=> !s.contains("OK")).top(10).mkString("\n"))
    var step = 0
    var change_par=true
    var continue: Boolean = true
    var newnum: Long = e.count()+n.count()
    var oldnum: Long = newnum
    /**
      * 开始迭代
      */

    while (continue) {
      t0 = System.nanoTime()
      step += 1
      println("\n************During step " + step + "************")
      /**
        * 计算
        */
      println("current partitions num:         \t"+n_edges.getNumPartitions)

      val t0_ge = System.nanoTime()
      val new_edges_str = {
        if(isSingleton==false){
          n_edges
            .mapPartitionsWithIndex((index, s) =>
              Graspan_OP.computeInPartition_fully_compressed_df_braodcast_e(step,
                index, s,e_bc.value.asInstanceOf[Array[(Int,Array[Int])]],
                nodes_num_bitsize,
                symbol_num_bitsize, is_complete_loop, max_complete_loop_turn,10,
                Batch_QueryHbase,
                htable_name,
                htable_split_Map,
                HRegion_splitnum, queryHBase_interval, default_split),true).setName("newedge-before-distinct-" + step)
        }
        else{
          val tmp_max_complete_loop_turn={
            if(newnum<10000) max_convergence_loop
            else max_complete_loop_turn
          }
          n_edges
            .mapPartitionsWithIndex((index, s) =>
              Graspan_OP.computeInPartition_fully_compressed_df_HDFSRead_E(step,
                index, s,master,e_str,
                nodes_num_bitsize,
                symbol_num_bitsize, is_complete_loop, tmp_max_complete_loop_turn,
                Batch_QueryHbase,
                htable_name,
                htable_split_Map,
                HRegion_splitnum, queryHBase_interval, default_split),true).setName("newedge-before-distinct-" + step)
        }
      }.persist (StorageLevel.MEMORY_ONLY_SER)
      val coarest_num=new_edges_str.map(s=>s._2._3).sum
      val t1_ge = System.nanoTime()
      println("old num:                        \t"+oldnum)
      //      println("oldedges_f num:                 \t"+ oldedges.map(s => (s._2._1._1.length)).sum().toLong)
      println("coarest num:                    \t"+coarest_num.toLong)
      println(s"generate new edges time:        \t ${((t1_ge - t0_ge)/1000000000.0).formatted("%.3f")} sec" )
      /**
        * 记录各分区情况
        */
      val par_INFO = new_edges_str.map(s=>s._2._2)
      deleteDir.deletedir(islocal, master, output + "/par_INFO/step" + step)
      par_INFO.repartition(1).saveAsTextFile(output + "/par_INFO/step" + step)

      val par_time_JOIN=par_INFO.map(s=>s.split("REPARJOIN")(1).trim.toDouble.toInt).collect().sorted
      println("Join take time Situation")
      println("Join Min Task take time         \t"+par_time_JOIN(0))
      println("Join 25% Task take time         \t"+par_time_JOIN((par_time_JOIN.length * 0.25).toInt))
      println("Join 50% Task take time         \t"+par_time_JOIN((par_time_JOIN.length * 0.5).toInt))
      println("Join 75% Task take time         \t"+par_time_JOIN((par_time_JOIN.length * 0.75).toInt))
      println("Join Max Task take time         \t"+par_time_JOIN(par_time_JOIN.length - 1))

      val par_time_HB=par_INFO.map(s=>s.split("REPARHBASE")(1).trim.toDouble.toInt).collect().sorted
      println("HBase take time Situation")
      println("HBase Min Task take time        \t"+par_time_HB(0))
      println("HBase 25% Task take time        \t"+par_time_HB((par_time_HB.length * 0.25).toInt))
      println("HBase 50% Task take time        \t"+par_time_HB((par_time_HB.length * 0.5).toInt))
      println("HBase 75% Task take time        \t"+par_time_HB((par_time_HB.length * 0.75).toInt))
      println("HBase Max Task take time        \t"+par_time_HB(par_time_HB.length - 1))

      /**
        * 新边去重
        */
      val t0_distinct=System.nanoTime():Double
      val newedges=new_edges_str.flatMapValues(s=>s._1).map(s=>s._2.toVector).distinct()
        .mapPartitions(s=>s.map(_.toArray)).setName("newedges-after-distinct-" + step).persist(StorageLevel
        .MEMORY_ONLY_SER)

      newnum = newedges.count()
      oldnum += newnum
      println("newedges:                       \t" + newnum)
      println("distinct take time:             \t" + ((System.nanoTime()-t0_distinct)/1000000000.0).formatted("%" +
        ".3f")+" secs")
      println("compute take time:              \t" + ((System.nanoTime()-t0)/1000000000.0).formatted("%.3f")+" secs")
      //      for(i<-symbol_Map){
      //        val symbol=i._1
      //        val symbol_num=i._2
      //        println(symbol+":                      \t"+newedges.filter(s=>s(2)==symbol_num).count()+",自环:"+newedges
      //          .filter(s=>s(2)==symbol_num&&s(0)==s(1)).count())
      //      }
      //        println("V:                              \t"+newedges_removedup.filter(s=>s._3==4).count())
      //        println("MAs:                            \t"+newedges_removedup.filter(s=>s._3==7).count())
      //        println("AMs:                            \t"+newedges_removedup.filter(s=>s._3==6).count())
      /**
        * Update HBase
        */
      val t0_hb = System.nanoTime(): Double
      deleteDir.deletedir(islocal, master, hbase_output)
      HBase_OP.updateHbase_java_flat(newedges, nodes_num_bitsize, symbol_num_bitsize, htable_name,
        hbase_output,
        htable_split_Map, HRegion_splitnum, default_split)
      val t1_hb = System.nanoTime(): Double
      println("update Hbase take time:         \t" + ((t1_hb - t0_hb) / 1000000000.0).formatted("%.3f") + " sec")

      new_edges_str.unpersist()

      /**
        * 更新旧边和新边
        */
      println("Start UNION")
      val t0_union = System.nanoTime()

      val cur_par=n_edges.getNumPartitions
      val need_par=(newnum/newnum_interval)*clusterpar

      val t0_new_flat_groupBykey=System.nanoTime()
      n_edges = {
        val origin=newedges.map(s=>(s(1),s(0))).groupByKey().mapValues(_.toArray)
        if(need_par>cur_par) origin.partitionBy(new HashPartitioner(need_par.toInt))
        else if(par_time_JOIN((par_time_JOIN.length * 0.75).toInt)*3 < par_time_JOIN(par_time_JOIN.length - 1)
          &&par_time_JOIN(par_time_JOIN.length-1)>30) origin.partitionBy(new HashPartitioner(cur_par.toInt))
        else origin
      }

      if(step % checkpoint_interval==0) {
        val t0_cp=System.nanoTime()
        n_edges.checkpoint()
        println("checkpoint take time:           \t"+((System.nanoTime()-t0_cp)/1000000000.0).formatted("%.3f")
          +" sec")
      }
      n_edges.count()

      println(s"n_edges time take time:          \t${((System.nanoTime() - t0_union)/1000000000.0).formatted("%" +
        ".3f")} sec")
      println("End UNION")

      t1 = System.nanoTime()
      println("*step: step " + step + " take time: \t " + ((t1 - t0) / 1000000000.0).formatted("%.3f") + " sec")
      println
      continue = newnum != 0
      //      scan.next()
    }

    sc.stop()
    println("final edges count():                     \t"+oldnum)
  }

}


