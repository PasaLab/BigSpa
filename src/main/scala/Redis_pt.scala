/**
  * Created by cycy on 2018/4/18.
  */
import java.text.SimpleDateFormat
import java.util
import java.util.Scanner
import java.util.concurrent.Executors

import cn.edu.nju.pasalab.db.ShardedRedisClusterClient
import org.apache.hadoop.conf.Configuration
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.storage.StorageLevel
import utils._

import scala.collection.mutable.ArrayBuffer

object Redis_pt extends Para{

  def main(args: Array[String]): Unit = {
    val t0_all=System.nanoTime()
    val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    println()
    println("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")
    print("@@  \t")
    println("Graspan Begin at "+df.format(System.currentTimeMillis())+"  \t@@")
    println("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")
    println()
    val scan = new Scanner(System.in)

    //    var t0=System.nanoTime():Double
    //    var t1=System.nanoTime():Double
    var islocal: Boolean = true
    var master: String = "local"

    var input_grammar: String = "data/test_grammar"
    var input_graph:String="data/test_graph"
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

    var updateRedis_interval:Int=50000
    var queryRedis_interval:Int=50000

    var is_complete_loop:Boolean=false
    var max_complete_loop_turn:Int=5
    var max_delta:Int=10000

    var file_index_f:Int= -1
    var file_index_b:Int= -1

    var check_edge:Boolean=false
    var outputdetails:Boolean=false
    var output_Par_INFO:Boolean=false

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
        case "clusterpar"=>clusterpar=argvalue.toInt
        case "defaultpar"=>defaultpar=argvalue.toInt
        case "newedges_interval"=>newedges_interval=argvalue.toInt

        case "openBloomFilter"=>openBloomFilter=argvalue.toBoolean
        case "edges_totalnum"=>edges_totalnum=argvalue.toInt
        case "error_rate"=>error_rate=argvalue.toDouble

        case "updateRedis_interval"=>updateRedis_interval=argvalue.toInt
        case "queryRedis_interval"=>queryRedis_interval=argvalue.toInt

        case "is_complete_loop"=>is_complete_loop=argvalue.toBoolean
        case "max_complete_loop_turn"=>max_complete_loop_turn=argvalue.toInt
        case "max_delta"=>max_delta=argvalue.toInt

        case "newnum_interval"=>newnum_interval=argvalue.toInt
        case "checkpoint_interval"=>checkpoint_interval=argvalue.toInt
        case "checkpoint_output"=>checkpoint_output=argvalue
        case "file_index_f"=>file_index_f=argvalue.toInt
        case "file_index_b"=>file_index_b=argvalue.toInt

        case "check_edge"=>check_edge=argvalue.toBoolean
        case "outputdetails"=>outputdetails=argvalue.toBoolean
        case "output_Par_INFO"=>output_Par_INFO=argvalue.toBoolean
        case _ => {}
      }
    }

    /**
      * 输出参数设置
      */



    /**
      * Spark 设置
      */
    val conf = new SparkConf()
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
    println("spark.executor.instances:     \t" + conf.get("spark.executor.instances"))
    println("spark.executor.cores:         \t" + conf.get("spark.executor.cores"))
    println("default partition num:        \t" + defaultpar)
    println("cluster partition num:        \t" + clusterpar)
    println("updateRedis_interval:          \t" + updateRedis_interval)
    println("queryRedis_interval:          \t" + queryRedis_interval)
    println("--------------------------------------------------------------------")
    println
    /**
      * Grammar相关设置
      */
    val grammar_origin = sc.textFile(input_grammar).filter(s=> !s.trim.equals("")).map(s => s.split("\\s+").map(_.trim))
      .collect().toList
    val (symbol_Map, symbol_num, symbol_num_bitsize, loop, directadd, grammar) = Graspan_OP.processGrammar(grammar_origin,
      input_grammar)
    println("------------Grammar INFO--------------------------------------------")
    println("input grammar:      \t" + input_grammar.split("/").last)
    println("symbol_num:         \t" + symbol_num)
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
    val (graph, nodes_num_bitsize, nodes_totalnum) = Graspan_OP.processGraph(sc, input_graph, file_index_f,file_index_b,
      input_grammar,
      symbol_Map, loop,
      directadd, defaultpar)

    println("------------Graph INFO--------------------------------------------")
    println("input graph:        \t" + input_graph.split("/").last)
    println("processed edges:    \t" + graph.count())
    println("nodes_totoalnum:    \t" + nodes_totalnum)
    println("nodes_num_bitsize:  \t" + nodes_num_bitsize)
    println("------------------------------------------------------------------")
    println


    /**
      * 原边集存入Redis
      */
    ShardedRedisClusterClient.getProcessLevelClient.clearDB()
    val redis_OP=new Redis_OP(updateRedis_interval)
    val t0_redis=System.nanoTime()
    redis_OP.Update(graph)
    println("Origin Update Redis take time:        \t"+((System.nanoTime()-t0_redis)/1000000000.0).formatted
    ("%.3f")+ "sec" )
    //    scan.next()

    deleteDir.deletedir(islocal, master, output)
    /**
      * 初始化oldedges
      */
    var newnum: Long = graph.count()
    var oldnum: Long = newnum
    var oldedges:RDD[(VertexId,(Array[Int],Array[Int],Array[Int],Array[Int],Array[Int]))]=null
    var oldedges_cogroup: RDD[(VertexId,(Iterable[(Array[Int],Array[Int],Array[Int],Array[Int],Array[Int])],
      Iterable[Array[Array[Int]]])
      )] =
      graph.flatMap(s => {
        if(s(0)!=s(1)) Array((s(0),s), (s(1),s))
        else Array((s(0),s))
      })
        .groupByKey()
        .map(s=>(s._1,({
          val old_f_list=new Array[Int](symbol_num)
          val new_f_list=new Array[Int](symbol_num)
          val old_b_list=new Array[Int](symbol_num)
          val new_b_list=new Array[Int](symbol_num)
          for(i <-0 until symbol_num){
            old_f_list(i)= -1
            new_f_list(i)= -1
            old_b_list(i)= -1
            new_b_list(i)= -1
          }
          Iterable((Array[Int](),old_f_list,new_f_list,old_b_list,new_b_list))
        },Iterable(s._2.toArray))))
        .partitionBy(new HashPartitioner(defaultpar))
    oldedges=oldedges_cogroup.mapPartitions((v=>Graspan_OP.Union(v,symbol_num)),true)
      .setName("oldedge-origin").persist(StorageLevel.MEMORY_ONLY_SER)
    oldedges.count()
    graph.unpersist()
    //    println("check oldedges")
    //    println(check_edge_RDD(oldedges).filter(s=> !s.contains("OK")).top(10).mkString("\n"))
    var step = 0
    var change_par=true
    var continue: Boolean = true
    println()
    val time_prepare=(System.nanoTime()-t0_all)/1000000000.0
    println("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")
    print("@@  \t")
    println("Compute Begin AT "+df.format(System.currentTimeMillis())+"  \t@@")
    println("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")
    println()
    val t0_all_2=System.nanoTime()
    /**
      * 开始迭代
      */
//    println("Please confirm to start compute!")
//    scan.next()
    while (continue) {
      val t0_turn = System.nanoTime()
      step += 1
      println("\n************During step " + step + "************")
      /**
        *1、 计算
        */
//      println("current partitions num:         \t"+oldedges_cogroup.getNumPartitions)
      val t0_ge = System.nanoTime()

      val new_edges_str = oldedges
        .mapPartitionsWithIndex((index, s) =>
          Graspan_OP.computeInPartition_pt(step,
            index, s,
            symbol_num,grammar,
            nodes_num_bitsize,
            symbol_num_bitsize, directadd,redis_OP),true).setName("newedge-before-distinct-" + step)
        .persist (StorageLevel.MEMORY_ONLY_SER)
      val coarest_num=new_edges_str.map(s=>s._2._3).sum
      val t1_ge = System.nanoTime()
      println("old num:                        \t"+oldnum)
      //      println("oldedges_f num:                 \t"+ oldedges.map(s => (s._2._1._1.length)).sum().toLong)
      println("coarest num:                    \t"+coarest_num.toLong)
      println(s"generate new edges time:        \t ${((t1_ge - t0_ge)/1000000000.0).formatted("%.3f")} sec" )

      /**
        * 2、新边去重
        */
      val t0_distinct=System.nanoTime():Double
      val newedges=new_edges_str.flatMapValues(s=>s._1).map(s=>s._2.toVector).distinct()
        .mapPartitions(s=>s.map(_.toArray)).setName("newedges-after-distinct-" + step).persist(StorageLevel
        .MEMORY_ONLY_SER)
      newnum = newedges.count()
      oldnum += newnum
      println("pure_newedges:                  \t" + newnum)
      println("distinct take time:             \t" + ((System.nanoTime()-t0_distinct)/1000000000.0).formatted("%" +
        ".3f")+" secs")
      println("compute take time:              \t" + ((System.nanoTime()-t0_turn)/1000000000.0).formatted("%.3f")+" " +
        "secs")
      /**
        * 3、记录各分区情况和产生的新边分布
        */
      val par_INFO = new_edges_str.map(s=>s._2._2).cache()
      deleteDir.deletedir(islocal, master, output + "/par_INFO/step" + step)
      par_INFO.repartition(1).saveAsTextFile(output + "/par_INFO/step" + step)
      var isnotBalance=false
      if(output_Par_INFO){
        val par_time_JOIN=par_INFO.map(s=>s.split("REPARJOIN")(1).trim.toDouble.toInt).collect().sorted
        println("Join take time Situation")
        println("Join Min Task take time         \t"+par_time_JOIN(0))
        println("Join 25% Task take time         \t"+par_time_JOIN((par_time_JOIN.length * 0.25).toInt))
        println("Join 50% Task take time         \t"+par_time_JOIN((par_time_JOIN.length * 0.5).toInt))
        println("Join 75% Task take time         \t"+par_time_JOIN((par_time_JOIN.length * 0.75).toInt))
        println("Join Max Task take time         \t"+par_time_JOIN(par_time_JOIN.length - 1))
        val par_time_HB=par_INFO.map(s=>s.split("REPARDB")(1).trim.toDouble.toInt).collect().sorted
        println("DB take time Situation")
        println("DB Min Task take time        \t"+par_time_HB(0))
        println("DB 25% Task take time        \t"+par_time_HB((par_time_HB.length * 0.25).toInt))
        println("DB 50% Task take time        \t"+par_time_HB((par_time_HB.length * 0.5).toInt))
        println("DB 75% Task take time        \t"+par_time_HB((par_time_HB.length * 0.75).toInt))
        println("DB Max Task take time        \t"+par_time_HB(par_time_HB.length - 1))

        isnotBalance=(par_time_JOIN((par_time_JOIN.length * 0.75).toInt)*3 < par_time_JOIN(par_time_JOIN.length - 1)&&
          par_time_JOIN(par_time_JOIN.length-1)>30)
//        for(i<-symbol_Map){
//          val symbol=i._1
//          val symbol_num=i._2
//          println(symbol+"    \t总数:    \t"+newedges.filter(s=>s(2)==symbol_num).count()
//            +"    \t自环:    \t"+newedges.filter(s=>s(2)==symbol_num&&s(0)==s(1)).count()
//            +"    \t非自:    \t"+newedges.filter(s=>s(2)==symbol_num&&s(0)!=s(1)).count())
//        }
      }
      /**
        * 4、Update Redis
        */
      val t0_hb = System.nanoTime(): Double
      deleteDir.deletedir(islocal, master, hbase_output)
      redis_OP.Update(newedges)
      val t1_hb = System.nanoTime(): Double
      println("update Redis take time:          \t" + ((t1_hb - t0_hb) / 1000000000.0).formatted("%.3f") + " sec")

      new_edges_str.unpersist()

      /**
        *5、更新旧边和新边
        */
      println("Start UNION")
      val t0_union = System.nanoTime()
      val cur_par=oldedges.getNumPartitions
      val oldEdgesPartitioner = oldedges_cogroup.partitioner.getOrElse(new HashPartitioner(cur_par))
      val need_par=(newnum/newnum_interval)*clusterpar
      val newedges_to_be_cogrouped = newedges.flatMap(s => {
        if(s(0)!=s(1)) Array((s(0),s),(s(1),s))
        else Array((s(0),s))
      }).groupByKey(oldEdgesPartitioner).mapValues(s=>s.toArray)
      oldedges_cogroup = {
        val oldedges_cogroup=
          oldedges.cogroup(newedges_to_be_cogrouped,oldEdgesPartitioner).setName("cogrouped-"+step)
        val tmp_oldedges=oldedges_cogroup
        if(need_par>cur_par){
          println("edges num is increasing, add Tasks")
          tmp_oldedges.partitionBy(new HashPartitioner(need_par.toInt))
        }
        else if(isnotBalance){
          println("Not Balance,repar")
          tmp_oldedges.partitionBy(new HashPartitioner(cur_par))
        }
        else{
          tmp_oldedges
        }
      }.setName("unioned-edges-" + step)//.persist(StorageLevel.MEMORY_ONLY_SER)
      oldedges=oldedges_cogroup.mapPartitions((v=>Graspan_OP.Union_old_new_improve(v,symbol_num)),true)
        .setName("oldedge-" + step).persist(StorageLevel.MEMORY_AND_DISK)

      if(step % checkpoint_interval==0) {
        val t0_cp=System.nanoTime()
        oldedges.checkpoint()
        println("checkpoint take time:           \t"+((System.nanoTime()-t0_cp)/1000000000.0).formatted("%.3f")
          +" sec")
      }
      oldedges.count()
      println(s"union time take time:            \t${((System.nanoTime() - t0_union)/1000000000.0).formatted("%.3f")}" +
        s" sec")
      println("End UNION")

      println("*step: step " + step + " take time: \t " + ((System.nanoTime() - t0_turn) / 1000000000.0).formatted("%.3f") + " sec")
      println
      continue = newnum != 0

      if(check_edge){
        scan.next()
      }
    }

    println("final edges count():                     \t"+oldnum)
    println()
    println("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")
    print("@@  \t")
    println("Graspan End at "+df.format(System.currentTimeMillis())+"  \t@@")
    println("compute take time " +(System.nanoTime()-t0_all_2)/1000000000.0+ " sec")
    println("prepare take time "+time_prepare+" sec")
    println("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")
//    println("Please confirm to end compute!")
//    scan.next()
    println()
    sc.stop()
  }

}


