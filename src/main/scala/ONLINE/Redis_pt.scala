package ONLINE

/**
  * Created by cycy on 2018/4/18.
  */

import java.text.SimpleDateFormat
import java.util.Scanner

import cn.edu.nju.pasalab.db.ShardedRedisClusterClient
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import utils_ONLINE._

import scala.collection.mutable.ArrayBuffer
import scala.io.Source

object Redis_pt {

  def main(args: Array[String]): Unit = {
    val t0_all = System.nanoTime()
    val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    println()
    println("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")
    print("@@  \t")
    println("Graspan Begin at " + df.format(System.currentTimeMillis()) + "  \t@@")
    println("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")
    println()
    val scan = new Scanner(System.in)

    //    var t0=System.nanoTime():Double
    //    var t1=System.nanoTime():Double

    /**
      * 参数设置
      */
    Param_pt.makeParams(args)
    /**
      * Spark 设置
      */
    val conf = new SparkConf()
    if (Param_pt.islocal) {
      //test location can be adjusted or not
      conf.setAppName("Graspan")
      System.setProperty("hadoop.home.dir", "F:/hadoop-2.6.0/")
      conf.setMaster("local")
    }
    val sc = new SparkContext(conf)
    sc.setCheckpointDir(Param_pt.checkpoint_output)
    //    try {
    println("------------Spark and HBase settings--------------------------------")
    println("spark.driver.memory:          \t" + conf.get("spark.driver.memory"))
    println("spark.executor.memory:        \t" + conf.get("spark.executor.memory"))
    println("spark.executor.instances:     \t" + conf.get("spark.executor.instances"))
    println("spark.executor.cores:         \t" + conf.get("spark.executor.cores"))
    println("default partition num:        \t" + Param_pt.defaultpar)
    println("cluster partition num:        \t" + Param_pt.clusterpar)
    println("updateRedis_interval:          \t" + Param_pt.updateRedis_interval)
    println("queryRedis_interval:          \t" + Param_pt.queryRedis_interval)
    println("--------------------------------------------------------------------")
    println
    /**
      * Grammar相关设置
      */
    val grammar_origin = sc.textFile(Param_pt.input_grammar).filter(s => !s.trim.equals("")).map(s => s.split("\\s+").map(_
      .trim))
      .collect().toList
    val (symbol_Map, symbol_num, symbol_num_bitsize, loop, directadd, grammar) = BIgSpa_OP.processGrammar(grammar_origin,
      Param_pt.input_grammar)
    println("------------Grammar INFO--------------------------------------------")
    println("input grammar:      \t" + Param_pt.input_grammar.split("/").last)
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
      * //      */
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

    /**
      * 原边集存入Redis
      */
    ShardedRedisClusterClient.getProcessLevelClient.clearDB()
    val redis_OP = new Redis_OP(Param_pt.updateRedis_interval)
    val t0_redis = System.nanoTime()
    //redis_OP.Update(graph)
    //    println("Origin Update Redis take time:        \t"+((System.nanoTime()-t0_redis)/1000000000.0).formatted
    //    ("%.3f")+ "sec" )
    //    scan.next()

    deleteDir.deletedir(Param_pt.islocal, Param_pt.master, Param_pt.output)
    deleteDir.deletedir(Param_pt.islocal,Param_pt.master,Param_pt.checkpoint_output)
    /**
      * 初始化oldedges
      */
    var oldedges: RDD[(Int, Node_Info)] = sc.parallelize(Array[(Int, Node_Info)]()).partitionBy(new HashPartitioner
    (Param_pt.clusterpar))

    //    println("check oldedges")
    //    println(check_edge_RDD(oldedges).filter(s=> !s.contains("OK")).top(10).mkString("\n"))

    var change_par = true
    var continue: Boolean = true
    println()
    val time_prepare = (System.nanoTime() - t0_all) / 1000000000.0
    println("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")
    print("@@  \t")
    println("Compute Begin AT " + df.format(System.currentTimeMillis()) + "  \t@@")
    println("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")
    println()
    val t0_all_2 = System.nanoTime()
    /**
      * 开始迭代
      */
    //    println("Please confirm to start compute!")
    //    scan.next()
    var total_num = 0L
    var step = 0
    /**
    val input_add = "data/Apache_httpd_2.2.18_pointsto_graph"
    val source=Source.fromFile(input_add).getLines()
    while (source.hasNext) {
      val buffer:ArrayBuffer[Array[String]]=new ArrayBuffer(1000)
      var i=0
      while(source.hasNext && i<Param_pt.input_interval) {
        buffer.append(source.next().split("\\s+"))
        i+=1
      }
      val add_edges: RDD[(Int, Int, Int)] = sc.parallelize(buffer).map(s => {
        (s(0).toInt, s(1).toInt, symbol_Map.getOrElse(s(2), -1))
      })
      */
    var tmp_add_edges_count:RDD[((Int,Int,Int),Int)]=null
    var tmp_addWorkList:RDD[(Int,Int,Int)]=null
    while (true) {
      val add_edges: RDD[(Int, Int, Int)] = sc.textFile(Param_pt.input_graph,Param_pt.clusterpar).map(line=>{
        val s=line.split("\\s+")
        (s(0).toInt,s(1).toInt,symbol_Map.getOrElse(s(2),-1))
      })
      val t0=System.nanoTime()
      /**
        * 1 添加边
        */
      /**
        * 1.1 合并 count
        */
      // add_edges 及其计数，用于更新old_edges计数
      var add_edges_count = add_edges.map(s => (s, 1)).reduceByKey((x, y) => x + y).persist(StorageLevel.MEMORY_ONLY_SER)
      //            println("add_edges_count: "+add_edges_count.collect().mkString(","))
      /**
        * 1.2 Query Redis，若不存在，加入AddWorkList
        */
      // addWorkList 为可能产生新的三角形的边，即为新边,不包括计数,
      var addWorkList: RDD[(Int, Int, Int)] = redis_OP.Query_PT(add_edges_count.map(s => s._1)).persist(StorageLevel
        .MEMORY_ONLY_SER)
      //      println("addWorkList: " + addWorkList.collect().mkString(","))

      var count_direction = 0
      println("\n************During step " + step + "************")
      tmp_add_edges_count=add_edges_count
      tmp_addWorkList=addWorkList
      var inner_step=0
      var addWorkList_count=addWorkList.count()
      var accumulate_newAdd=0L
      while (addWorkList_count > 0) {
        println(" -- inner step: "+inner_step)
        println("addWorkList.count:  \t"+addWorkList_count)
        total_num += addWorkList_count
        accumulate_newAdd+=addWorkList_count
        /**
          * 1.3 计算下一轮的新边
          */
        // addWorkList 组织为midnode为key的RDD形式，与

        val add_edges_groupbymid: RDD[(Int, Iterable[(Int, Int, Int)])] = addWorkList.flatMap(s => {
          if (s._1 == s._2) Array((s._1, s))
          else Array((s._1, s), (s._2, s))
        }).groupByKey()
        //          .persist(StorageLevel.MEMORY_ONLY).setName("add_edges_groupbymid"+inner_step)
        //        add_edges_groupbymid.checkpoint()
        //        add_edges_groupbymid.count()
        //        println("add_edges_groupbymid: " + add_edges_groupbymid.collect().mkString("\n"))
        //        println("add_edges_groupbymid.leftOuterJoin(oldedges): " + add_edges_groupbymid.leftOuterJoin(oldedges).collect
        //        ().mkString("\n"))
        //        print("compute nextAddEdges ? ")
        //        scan.next()
        //        println("add_edges_groupbymid.isCheckpointed: "+add_edges_groupbymid.isCheckpointed)
        val nextAddEdges: RDD[(Int, Int, Int)] = add_edges_groupbymid
          .leftOuterJoin(oldedges,oldedges.partitioner.getOrElse(new HashPartitioner(Param_pt.clusterpar)))
          .mapPartitionsWithIndex((index, p) => BIgSpa_OP.computeInPartition_pt_predictNextEdges(index, p, symbol_num, loop, directadd, grammar))
          .persist(StorageLevel.MEMORY_ONLY_SER)
          .setName("nextAddEdges"+inner_step)
        //        nextAddEdges.checkpoint()
        println("nextAddEdges's top : " + nextAddEdges.take(1))



        //        println("nextAddEdges.isCheckedpointd? "+nextAddEdges.isCheckpointed)
        /**
          * 1.4 更新 oldedges count
          */
        val add_edges_count_groupbymid: RDD[(Int, Iterable[((Int, Int, Int), Int)])] = add_edges_count.flatMap(s => {
          if (s._1._1 == s._1._2) Array((s._1._1, s))
          else Array((s._1._1, s), (s._1._2, s))
        }).groupByKey()
        var old_partitioner=oldedges.partitioner.getOrElse(new HashPartitioner(Param_pt.clusterpar))
        val tmp_oldedges=oldedges
        val tmp_oldedges_partitionInfo= oldedges.fullOuterJoin(add_edges_count_groupbymid,old_partitioner)
          .mapPartitionsWithIndex(
            (index, p) => BIgSpa_OP
              .Update_Count_inPartition(index, p, symbol_num, grammar, count_direction),true).persist(StorageLevel
          .MEMORY_AND_DISK_SER)
        //        println("tmp_old_par: "+tmp_oldedges_partitionInfo.count())
        val partition_num=tmp_oldedges_partitionInfo.map(s=>s._2._2).collect().sorted
        println("Partition edges Situation")
        println("Partition edges Min has         \t"+partition_num(0))
        println("Partition edges 25% has         \t"+partition_num((partition_num.length * 0.25)
          .toInt))
        println("Partition edges 50% has         \t"+partition_num((partition_num.length * 0.5).toInt))
        println("Partition edges 75% has         \t"+partition_num((partition_num.length * 0.75).toInt))
        println("Partition edges Max has         \t"+partition_num(partition_num.length - 1))

        oldedges=tmp_oldedges_partitionInfo.flatMapValues(s=>s._1).mapPartitions(s=>s.map(x=>x._2),
          preservesPartitioning = true)
          .persist (StorageLevel.MEMORY_AND_DISK_SER)
          .setName("oldedges"+inner_step)
        if(partition_num((partition_num.length * 0.75).toInt)*3 < partition_num(partition_num.length - 1)) {
          println("repartition")
          old_partitioner = new HashPartitioner(Param_pt.clusterpar)
          oldedges=oldedges.partitionBy(old_partitioner)
        }
        //        oldedges.checkpoint()
        //        println("oldedges's top? "+oldedges.take(1))
        /**
          * 1.5 将 AddWorkList 添加到Redis
          */
        redis_OP.Update(addWorkList)

        /**
          * 1.1 合并 count
          */
        add_edges_count = nextAddEdges.map(s => (s, 1)).reduceByKey((x, y) => x + y).persist(StorageLevel
          .MEMORY_ONLY_SER).setName("add_edges_count"+inner_step)
        //        if(inner_step%Param_pt.checkpoint_interval==Param_pt.checkpoint_interval-1)
        add_edges_count.checkpoint()
        //        println("add_edges_count.isCheckpointed: "+add_edges_count.isCheckpointed)
        //        add_edges_count.count()
        //        println("add_edges_count‘: " + add_edges_count.count())

        /**
          * 1.2 Query Redis，若不存在，加入AddWorkList
          */
        addWorkList = redis_OP.Query_PT(add_edges_count.map(s => s._1))
          .persist(StorageLevel.MEMORY_ONLY_SER)
          .setName("addWorkList"+inner_step)
        //        if(inner_step%Param_pt.checkpoint_interval==0)
        addWorkList.checkpoint()
        addWorkList_count=addWorkList.count()
        //        println("add_edges_count.isCheckpointed: "+add_edges_count.isCheckpointed)
        //        println("addWorkList.isCheckpointed: "+addWorkList.isCheckpointed)
        //        println("addWorkList’: " + addWorkList.collect().mkString(","))
        //更新count_direction, 只有第一轮添加的直接计数，之后都是三角计数
        count_direction |= 1
        inner_step+=1


        if(inner_step%Param_pt.checkpoint_interval==0) {
          //          if (accumulate_newAdd > 1e6) {
          //            println(s"inner step: $inner_step , accumulat_newAdd: $accumulate_newAdd , oldedges repartition")
          //            oldedges = oldedges.repartition(Param_pt.clusterpar).persist(StorageLevel.MEMORY_AND_DISK)
          //          }
          oldedges.checkpoint()
        }
        println("oldedges action "+oldedges.take(1))
        tmp_oldedges.unpersist()
        tmp_oldedges_partitionInfo.unpersist()
        tmp_add_edges_count.unpersist()
        tmp_addWorkList.unpersist()
        //
        //        printEdges(oldedges)
        print("pause? ")
        scan.next()
      }
      val t1=System.nanoTime()
      step += 1
      println("total_num:  \t" + total_num)
      println(s"inner step: \t$inner_step")
      println("takes time: \t"+(t1-t0)/1e9)
      //      if(step%Param_pt.checkpoint_interval==0) oldedges.checkpoint()
      //      printEdges(oldedges)
      /**
        * 移除边
        */
      //      val remove_edges: RDD[(Int, Int, Int)] = null
    }

    //    println("final edges count():                     \t"+oldnum)
    println()
    println("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")
    print("@@  \t")
    println("Graspan End at " + df.format(System.currentTimeMillis()) + "  \t@@")
    println("compute take time " + (System.nanoTime() - t0_all_2) / 1000000000.0 + " sec")
    println("prepare take time " + time_prepare + " sec")
    println("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")
    //    println("Please confirm to end compute!")
    //    scan.next()
    println()
    sc.stop()
  }

  def printEdges(edges: RDD[(Int, Node_Info)]): Unit = {
    edges.collect().map(s => {
      println(s._1 + " - ")
      s._2.print()
    })
  }
}


