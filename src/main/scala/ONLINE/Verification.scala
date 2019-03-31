package ONLINE

/**
  * Created by cycy on 2019/3/30.
  */

import java.io.File
import java.text.SimpleDateFormat
import java.util.Scanner

import OFFLINE.utils_OFFLINE
import ONLINE.utils_ONLINE.Param_pt._
import OFFLINE.utils_OFFLINE.{BIgSpa_OP, Param_pt, Redis_OP}
import ONLINE.utils_ONLINE.{Param_pt, deleteDir}
import cn.edu.nju.pasalab.db.ShardedRedisClusterClient
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

import scala.io.Source

object Verification{

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

    /**
      * 参数设置
      */
    makeParams(args)
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
    val grammar_origin = sc.textFile(input_grammar).filter(s=> !s.trim.equals("")).map(s => s.split("\\s+").map(_
      .trim))
      .collect().toList
    val (symbol_Map, symbol_num, symbol_num_bitsize, loop, directadd, grammar) = BIgSpa_OP.processGrammar(grammar_origin,
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
    grammar.foreach(s => println("                    \t" + s(0) + "\t+\t" + s(1) + "\t->\t" + s(2)))
    println("---------------------------------------------------------------------")
    println

    /**
      * Graph相关设置
      */
//    val (graph, nodes_num_bitsize, nodes_totalnum) = BIgSpa_OP.processGraph(sc, input_graph, file_index_f,
//      file_index_b,
//      input_grammar,
//      symbol_Map, loop,
//      directadd, defaultpar)

//    println("------------Graph INFO--------------------------------------------")
//    println("input graph:        \t" + input_graph.split("/").last)
//    println("processed edges:    \t" + graph.count())
//    println("nodes_totoalnum:    \t" + nodes_totalnum)
//    println("nodes_num_bitsize:  \t" + nodes_num_bitsize)
//    println("------------------------------------------------------------------")
//    println


    /**
      * 原边集存入Redis
      */
    ShardedRedisClusterClient.getProcessLevelClient.clearDB()
    val redis_OP=new Redis_OP(updateRedis_interval)
//    val t0_redis=System.nanoTime()
//    redis_OP.Update(graph)
//    println("Origin Update Redis take time:        \t"+((System.nanoTime()-t0_redis)/1000000000.0).formatted
//    ("%.3f")+ "sec" )
    //    scan.next()

    deleteDir.deletedir(islocal, master, output)
    /**
      * 初始化oldedges
      */


    var oldedges:RDD[(Int,(Array[Int],Array[Int],Array[Int],Array[Int],Array[Int]))]=sc.parallelize(Array[(Int,(Array[Int],Array[Int],Array[Int],Array[Int],Array[Int]))]())
    var oldedges_cogroup: RDD[(Int,(Iterable[(Array[Int],Array[Int],Array[Int],Array[Int],Array[Int])],
      Iterable[Array[Array[Int]]]))] =sc.parallelize(Array[(Int,(Iterable[(Array[Int],Array[Int],Array[Int],Array[Int],Array[Int])],
      Iterable[Array[Array[Int]]]))]())

    oldedges=oldedges_cogroup.mapPartitions((v=>BIgSpa_OP.Union(v,symbol_num)),true)
      .setName("oldedge-origin").persist(StorageLevel.MEMORY_ONLY_SER)
    oldedges.count()
    //    println("check oldedges")
    //    println(check_edge_RDD(oldedges).filter(s=> !s.contains("OK")).top(10).mkString("\n"))

//    var change_par=true
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
    val f = new File(add).listFiles.filter(_.isFile).toIterator
    var oldnum=0l
    var addturn=0
    for(add<-f) {
      val oldpartitioner_FromFile=oldedges.partitioner.getOrElse(new HashPartitioner(clusterpar))
      val newedges_to_be_cogrouped_FormFile :RDD[(Int,Array[Array[Int]])]= sc.parallelize(Source.fromFile(add)
        .getLines().toArray[String])
        .map(line => {
        val s = line.split("\\s+")
        Array[Int](s(0).toInt, s(1).toInt, symbol_Map.getOrElse(s(2), -1))
      }).flatMap(s => {
        if (s(0) != s(1)) Array((s(0), s), (s(1), s))
        else Array((s(0), s))
      }).groupByKey(oldpartitioner_FromFile).mapValues(s => s.toArray)
      val oldedges_cogroup_FromFile = oldedges.cogroup(newedges_to_be_cogrouped_FormFile, oldpartitioner_FromFile)
        .setName("cogrouped-flow") //.persist(StorageLevel.MEMORY_ONLY_SER)

      oldedges = oldedges_cogroup_FromFile.mapPartitions((v => BIgSpa_OP.Union(v, symbol_num)), true)
        .setName("oldedge-flow").persist(StorageLevel.MEMORY_AND_DISK)
      oldedges.checkpoint()
      oldedges.take(1)
      var flowadd=Source.fromFile(add).getLines().toArray.length.toLong
      var step = 0
      var newnum: Long = 0l
      var continue: Boolean = true
      println("\n************During add turn " + addturn + "************")
      while (continue) {
        val t0_turn = System.nanoTime()
        step += 1
        /**
          * 1、 计算
          */
        //      println("current partitions num:         \t"+oldedges_cogroup.getNumPartitions)
        val t0_ge = System.nanoTime()

        val new_edges_str = oldedges
          .mapPartitionsWithIndex((index, s) =>
            BIgSpa_OP.computeInPartition_pt(step,
              index, s,
              symbol_num, grammar,
              0,
              symbol_num_bitsize, directadd, redis_OP), true).setName("newedge-before-distinct-" + step)
          .persist(StorageLevel.MEMORY_ONLY_SER)
        val coarest_num = new_edges_str.map(s => s._2._3).sum
        val t1_ge = System.nanoTime()
//        println("old num:                        \t" + oldnum)
        //      println("oldedges_f num:                 \t"+ oldedges.map(s => (s._2._1._1.length)).sum().toLong)

//        println(s"generate new edges time:        \t ${((t1_ge - t0_ge) / 1000000000.0).formatted("%.3f")} sec")

        /**
          * 2、新边去重
          */
        val t0_distinct = System.nanoTime(): Double
        val newedges = new_edges_str.flatMapValues(s => s._1).map(s => s._2.toVector).distinct()
          .mapPartitions(s => s.map(_.toArray)).setName("newedges-after-distinct-" + step).persist(StorageLevel
          .MEMORY_ONLY_SER)
        newnum = newedges.count()
//        oldnum += newnum
        println(s"in step $step coarest num:\t"+ coarest_num.toLong+"\tpure_newedges: \t" + newnum)
//        if(addturn==617){
//          println("add_edges:")
//          newedges.collect().foreach(e=>println(e(0)+"\t"+e(1)+"\t"+e(2)))
//        }
        flowadd+=newnum
//        println("distinct take time:             \t" + ((System.nanoTime() - t0_distinct) / 1000000000.0).formatted("%" +
//          ".3f") + " secs")
//        println("compute take time:              \t" + ((System.nanoTime() - t0_turn) / 1000000000.0).formatted("%.3f") + " " +
//          "secs")
        /**
          * 3、记录各分区情况和产生的新边分布
          */
//        val par_INFO = new_edges_str.map(s => s._2._2).cache()
//        deleteDir.deletedir(islocal, master, output + "/par_INFO/step" + step)
//        par_INFO.repartition(1).saveAsTextFile(output + "/par_INFO/step" + step)
//        var isnotBalance = false
//        if (output_Par_INFO) {
//          val par_time_JOIN = par_INFO.map(s => s.split("REPARJOIN")(1).trim.toDouble.toInt).collect().sorted
//          println("Join take time Situation")
//          println("Join Min Task take time         \t" + par_time_JOIN(0))
//          println("Join 25% Task take time         \t" + par_time_JOIN((par_time_JOIN.length * 0.25).toInt))
//          println("Join 50% Task take time         \t" + par_time_JOIN((par_time_JOIN.length * 0.5).toInt))
//          println("Join 75% Task take time         \t" + par_time_JOIN((par_time_JOIN.length * 0.75).toInt))
//          println("Join Max Task take time         \t" + par_time_JOIN(par_time_JOIN.length - 1))
//          val par_time_HB = par_INFO.map(s => s.split("REPARDB")(1).trim.toDouble.toInt).collect().sorted
//          println("DB take time Situation")
//          println("DB Min Task take time        \t" + par_time_HB(0))
//          println("DB 25% Task take time        \t" + par_time_HB((par_time_HB.length * 0.25).toInt))
//          println("DB 50% Task take time        \t" + par_time_HB((par_time_HB.length * 0.5).toInt))
//          println("DB 75% Task take time        \t" + par_time_HB((par_time_HB.length * 0.75).toInt))
//          println("DB Max Task take time        \t" + par_time_HB(par_time_HB.length - 1))
//
//          isnotBalance = (par_time_JOIN((par_time_JOIN.length * 0.75).toInt) * 3 < par_time_JOIN(par_time_JOIN.length - 1) &&
//            par_time_JOIN(par_time_JOIN.length - 1) > 30)
//          //        for(i<-symbol_Map){
//          //          val symbol=i._1
//          //          val symbol_num=i._2
//          //          println(symbol+"    \t总数:    \t"+newedges.filter(s=>s(2)==symbol_num).count()
//          //            +"    \t自环:    \t"+newedges.filter(s=>s(2)==symbol_num&&s(0)==s(1)).count()
//          //            +"    \t非自:    \t"+newedges.filter(s=>s(2)==symbol_num&&s(0)!=s(1)).count())
//          //        }
//        }
        /**
          * 4、Update Redis
          */
        val t0_hb = System.nanoTime(): Double
        deleteDir.deletedir(islocal, master, hbase_output)
        redis_OP.Update(newedges)
        val t1_hb = System.nanoTime(): Double
//        println("update Redis take time:          \t" + ((t1_hb - t0_hb) / 1000000000.0).formatted("%.3f") + " sec")

        new_edges_str.unpersist()

        /**
          * 5、更新旧边和新边
          */
//        println("Start UNION")
        val t0_union = System.nanoTime()
        val cur_par = oldedges.getNumPartitions
        val oldEdgesPartitioner = oldedges_cogroup.partitioner.getOrElse(new HashPartitioner(cur_par))
        val need_par = (newnum / newnum_interval) * clusterpar
        val newedges_to_be_cogrouped = newedges.flatMap(s => {
          if (s(0) != s(1)) Array((s(0), s), (s(1), s))
          else Array((s(0), s))
        }).groupByKey(oldEdgesPartitioner).mapValues(s => s.toArray)
        oldedges_cogroup = {
          val oldedges_cogroup =
            oldedges.cogroup(newedges_to_be_cogrouped, oldEdgesPartitioner).setName("cogrouped-" + step)
          val tmp_oldedges = oldedges_cogroup
          if (need_par > cur_par) {
//            println("edges num is increasing, add Tasks")
            tmp_oldedges.partitionBy(new HashPartitioner(need_par.toInt))
          }
          else {
            tmp_oldedges
          }
        }.setName("unioned-edges-" + step) //.persist(StorageLevel.MEMORY_ONLY_SER)


        oldedges = oldedges_cogroup.mapPartitions((v => BIgSpa_OP.Union(v, symbol_num)), true)
          .setName("oldedge-" + step).persist(StorageLevel.MEMORY_AND_DISK)

        if (step %checkpoint_interval== 0) {
          println("checkpoint")
          oldedges.checkpoint()
          //          println("checkpoint take time:           \t" + ((System.nanoTime() - t0_cp) / 1000000000.0).formatted("%.3f")
          //            + " sec")
        }
        oldedges.count()
//        println(s"union time take time:            \t${((System.nanoTime() - t0_union) / 1000000000.0).formatted("%.3f")}" +
//          s" sec")
//        println("End UNION")

//        println("*step: step " + step + " take time: \t " + ((System.nanoTime() - t0_turn) / 1000000000.0).formatted("%.3f") + " sec")
//        println

        continue = newnum != 0

        if (check_edge) {
          scan.next()
        }
      }
      oldnum+=flowadd
      println(s"addturn\t$addturn\taddfile\t$add\tflowadd\t$flowadd\ttotaladd\t$oldnum")
      addturn+=1
//      if(addturn==617||addturn==618) scan.next()
//      if (addturn % checkpoint_interval == 0) {
//        val t0_cp = System.nanoTime()
//        oldedges.checkpoint()
//        //          println("checkpoint take time:           \t" + ((System.nanoTime() - t0_cp) / 1000000000.0).formatted("%.3f")
//        //            + " sec")
//      }
//      scan.next()
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


