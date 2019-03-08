package ONLINE

/**
  * Created by cycy on 2018/4/18.
  */

import java.text.SimpleDateFormat
import java.util.Scanner

import ONLINE.utils_ONLINE._
import cn.edu.nju.pasalab.db.ShardedRedisClusterClient
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer
import scala.io.Source

object Redis_pt_all {

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
      * 设置Redis
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

    var tmp_add_edges_count:RDD[((Int,Int,Int),Int)]=null
    var tmp_addWorkList: RDD[(Int, Int, Int)]=null
    val t0=System.nanoTime()
    /**
      * 1 添加边
      */
    /**
      * 1.1 合并 count
      */
    // add_edges 及其计数，用于更新old_edges计数
    var add_edges_count :RDD[((Int,Int,Int),Int)]= sc.textFile(Param_pt.input_graph,Param_pt.clusterpar)
      .filter(!_.trim.equals(""))
      .map(line=>{
        val s=line.split("\\s+")
        ((s(0).toInt,s(1).toInt,symbol_Map.getOrElse(s(2),-1)),1)
      }).reduceByKey((x, y) => x + y).persist(StorageLevel.MEMORY_AND_DISK_SER)
    //            println("add_edges_count: "+add_edges_count.collect().mkString(","))
    /**
      * 1.2 Query Redis，若不存在，加入AddWorkList
      */
    // addWorkList 为可能产生新的三角形的边，即为新边,不包括计数,
    var addWorkList= redis_OP.Query_PT(add_edges_count.map(s => s._1)).persist(StorageLevel.MEMORY_AND_DISK_SER)
    //      println("addWorkList: " + addWorkList.collect().mkString(","))
    //    scan.next()
    var count_direction = 0

    tmp_add_edges_count=add_edges_count
    tmp_addWorkList=addWorkList
    var inner_step=0
    var addWorkList_count=addWorkList.count()
    //    scan.next()
    var accumulate_newAdd=0L
    while (addWorkList_count > 0) {
      println("\n************During inner_step " + inner_step + "************")
      println("addWorkList.count:  \t"+addWorkList_count)
      tmp_add_edges_count=add_edges_count
      tmp_addWorkList=addWorkList
      total_num += addWorkList_count
      accumulate_newAdd+=addWorkList_count
      /**
        * 1.3 计算下一轮的新边
        */
      // addWorkList 组织为midnode为key的RDD形式，与

      val add_edges_groupbymid = addWorkList.flatMap(s => {
        if (s._1 == s._2) Array((s._1, s))
        else Array((s._1, s), (s._2, s))
      }).groupByKey()
      val nextAddEdges: RDD[(Int, Int, Int)] = add_edges_groupbymid
        .leftOuterJoin(oldedges,oldedges.partitioner.getOrElse(new HashPartitioner(Param_pt.clusterpar)))
        .mapPartitionsWithIndex((index, p) => BIgSpa_OP.computeInPartition_pt_predictNextEdges(index, p, symbol_num, loop, directadd, grammar))
        .persist(StorageLevel.MEMORY_AND_DISK_SER)
        .setName("nextAddEdges"+inner_step)
      println("nextAddEdges's top :  \t" + nextAddEdges.take(1))
      //        scan.next()

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

      val partition_num=tmp_oldedges_partitionInfo.map(s=>s._2._2).collect().sorted
      //        scan.next()
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
      if(partition_num((partition_num.length * 0.75).toInt)*3 < partition_num(partition_num.length - 1)
        || (inner_step % Param_pt.checkpoint_interval==0 && total_num >8e8)
      ) {
        println("repartition")
        old_partitioner = new HashPartitioner(Param_pt.clusterpar)
        oldedges=oldedges.partitionBy(old_partitioner)
      }

      /**
        * 1.5 将 AddWorkList 添加到Redis
        */
      redis_OP.Update(addWorkList)
      //        scan.next()
      /**
        * 1.1 合并 count
        */
      add_edges_count = nextAddEdges.map(s => (s, 1)).reduceByKey((x, y) => x + y).persist(StorageLevel
        .MEMORY_AND_DISK_SER).setName("add_edges_count"+inner_step)
      add_edges_count.checkpoint()

      /**
        * 1.2 Query Redis，若不存在，加入AddWorkList
        */
      addWorkList = redis_OP.Query_PT(add_edges_count.map(s => s._1))
        .persist(StorageLevel.MEMORY_AND_DISK_SER)
        .setName("addWorkList"+inner_step)
      addWorkList_count=addWorkList.count()
      addWorkList.checkpoint()
      //        scan.next()
      //更新count_direction, 只有第一轮添加的直接计数，之后都是三角计数
      count_direction |= 1
      inner_step+=1


      if(inner_step%Param_pt.checkpoint_interval==0) {
        oldedges.checkpoint()
      }
      println("oldedges action  \t"+oldedges.take(1))
      //        scan.next()
      tmp_oldedges.unpersist()
      tmp_oldedges_partitionInfo.unpersist()
      tmp_add_edges_count.unpersist()
      tmp_addWorkList.unpersist()
      //        nextAddEdges.unpersist()

      //
      //        printEdges(oldedges)
      //        print("pause? ")
      //        scan.next()
      println("total_num:  \t" + total_num)
    }
    val t1=System.nanoTime()
    step += 1
    println("total_num:  \t" + total_num)
    println(s"inner step: \t$inner_step")
    println("takes time: \t"+(t1-t0)/1e9)


    //    while(true){
    //      println("please input your add way:\n \t 1. input tuples src dst label unitl ending \n \t 2. input file path ")
    //      val way=scan.nextInt()
    //      val buffer:ArrayBuffer[(Int,Int,Int)]=new ArrayBuffer(1000)
    //      way match {
    //        case 1 =>{
    //          var i=0
    //          var (src,dst,label)=(scan.nextInt(),scan.nextInt(),scan.nextInt())
    //          while(src != -1) {
    //            buffer.append((src,dst,label))
    //            src=scan.nextInt()
    //            dst=scan.nextInt()
    //            label=scan.nextInt()
    //          }
    //        }
    //        case 2 =>{
    //          val input_add = scan.nextLine()
    //          val source=Source.fromFile(input_add).getLines()
    //          while (source.hasNext) {
    //            val s=source.next().split("\\s+").map(_.toInt)
    //              buffer.append((s(0),s(1),s(2)))
    //            }
    //        }
    //      }
    //      val add_edges: RDD[(Int, Int, Int)] = sc.parallelize(buffer)
    //
    //    }
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

    /**
      * 存储已计算的蝴蝶型结构和count信息
      */
    oldedges.map(s=>(s._1+"-"+s._2.print())).saveAsTextFile(Param_pt.output+"/alledgesStates")
    /**
      * 统计所有节点的neighbours个数 和 某label下最大边数
      */

    val count_rdd=oldedges.map(s=>(s._1,(s._2.calNeighboursTotalNum(),s._2.calLabelEdgesMaximum())))
    println("Max calNeighboursTotalNum: \t "+count_rdd.map(s=>s._2._1).max())
    println("Max calLabelEdgesMaximum:  \t "+count_rdd.map(s=>s._2._2).max())
    count_rdd.map(s=>(s._1+":"+s._2._1+","+s._2._2)).repartition(1).saveAsTextFile(Param_pt.output+"/edgesNumTongji")



    sc.stop()
  }

  def printEdges(edges: RDD[(Int, Node_Info)]): Unit = {
    edges.collect().map(s => {
      println(s._1 + " - ")
      println(s._2.print())
    })
  }
}


