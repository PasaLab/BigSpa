package OFFLINE

/**
  * Created by cycy on 2018/4/18.
  */

import java.text.SimpleDateFormat
import java.util.Scanner

import cn.edu.nju.pasalab.db.ShardedRedisClusterClient
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import utils_OFFLINE._

object Redis_df {

  def main(args: Array[String]): Unit = {
    val t0_all=System.nanoTime()
    val scan = new Scanner(System.in)
    val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    println()
    println("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")
    print("@@  \t")
    println("Graspan Begin at "+df.format(System.currentTimeMillis())+"  \t@@")
    println("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")
    println()

    Param_df.makeParams(args)

    /**
      * Spark 设置
      */
    val conf = new SparkConf()//.set("spark.kryoserializer.buffer.max", "2048")
    if (Param_df.islocal) {
      //test location can be adjusted or not
      conf.setAppName("Graspan")
      System.setProperty("hadoop.home.dir", "F:/hadoop-2.6.0/")
      conf.setMaster("local")
    }
    val sc = new SparkContext(conf)
    sc.setCheckpointDir(Param_df.checkpoint_output)
    //    try {
    println("------------Spark and HBase settings--------------------------------")
    println("spark.driver.memory:          \t" + conf.get("spark.driver.memory"))
    println("spark.executor.memory:        \t" + conf.get("spark.executor.memory"))
    println("spark.executor.instances:     \t" + conf.get("spark.executor.instances"))
    println("spark.executor.cores:         \t" + conf.get("spark.executor.cores"))
    println("default partition num:        \t" + Param_df.defaultpar)
    println("cluster partition num:        \t" + Param_df.clusterpar)
    println("queryRedis_interval:          \t" + Param_df.queryRedis_interval)
    println("updateRedis_interval:         \t" + Param_df.updateRedis_interval)
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
      if(Param_df.input_e.contains("Linux_dataflow_e")){
        println("getinput_EandN")
        BIgSpa_OP.getLinux_input_EandN(Param_df.input_e,Param_df.input_n,Param_df.file_index_f,Param_df.file_index_b,Param_df.master)
      }
      else{
        (Param_df.input_e,Param_df.input_n)
      }
    }


    val e=sc.textFile(e_str,Param_df.defaultpar).flatMap(s=>{
      val flag=s.split(":")(0).toInt
      s.split(":")(1).split("\\s+").map(x=>Array(flag,x.toInt,1))
    })
    val n=sc.textFile(n_str,Param_df.defaultpar).map(s=>s.split("\\s+").map(_.toInt))
    println("e counts : "+e.filter(s=>s(2)==1).count())
    println("n counts : "+n.filter(s=>s(2)==0).count())
    val nodes_totalnum=(e.flatMap(s=>Array(s(0),s(1))) ++ n.flatMap(s=>Array(s(0),s(1)))).distinct().count()
    val nodes_num_bitsize=BIgSpa_OP.getIntBit(nodes_totalnum.toInt)
    println("------------Graph INFO--------------------------------------------")
    println("input graph:        \t" + Param_df.input_e)
    println("processed e:        \t" + e.count())
    println("processed n:        \t" + n.count())
    println("nodes_totoalnum:    \t" + nodes_totalnum)
    println("nodes_num_bitsize:  \t" + nodes_num_bitsize)
    println("max_loop_turn:      \t" + Param_df.max_complete_loop_turn)
    println("convergence_turn:   \t" + Param_df.max_convergence_loop)
    println("------------------------------------------------------------------")
    println

    /**
      * 原边集存入Redis
      */
    val t0_init_redis_origin=System.nanoTime()
    ShardedRedisClusterClient.getProcessLevelClient.clearDB()
    val t0_redis=System.nanoTime()
    val redis_OP=new Redis_OP(Param_df.updateRedis_interval)
    redis_OP.Update(e)
    redis_OP.Update(n)
    println("Origin Update Redis take time:        \t"+((System.nanoTime()-t0_redis)/1000000000.0).formatted
    ("%.3f")+ "sec" )
    //    scan.next()

    deleteDir.deletedir(Param_df.islocal,Param_df.master, Param_df.output)

    var n_edges=n.map(s=>(s(1),s(0))).groupByKey().mapValues(_.toArray)
      .partitionBy(new HashPartitioner(Param_df.defaultpar))
      .persist(StorageLevel.MEMORY_ONLY_SER)
    //    oldedges.count()

    //    println("check oldedges")
    //    println(check_edge_RDD(oldedges).filter(s=> !s.contains("OK")).top(10).mkString("\n"))
    var step = 0
    var change_par=true
    var continue: Boolean = true
    var newnum: Long = e.count()+n.count()
    var oldnum: Long = newnum
    val init_e=n_edges.mapPartitionsWithIndex((index,s)=>BIgSpa_OP.init_e(index,s,Param_df.master,e_str))
    println("Init e in "+init_e.count()+"Partitions")
    val time_init_redis_origin=System.nanoTime()-t0_init_redis_origin
    val time_prepare=(System.nanoTime()-t0_all)/1000000000.0
    println()
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
        * 计算
        */
//      println("current partitions num:         \t"+n_edges.getNumPartitions)
      val t0_ge = System.nanoTime()
      val new_edges_str = {
        val tmp_max_complete_loop_turn={
          if(newnum<=Param_df.convergence_threshold) Param_df.max_convergence_loop
          else Param_df.max_complete_loop_turn
        }
        n_edges
          .mapPartitionsWithIndex((index, s) =>
            BIgSpa_OP.computeInPartition_df(step,
              index, s,Param_df.master,e_str,
              nodes_num_bitsize,
              symbol_num_bitsize, Param_df.is_complete_loop, tmp_max_complete_loop_turn,redis_OP),true).setName("newedge-before-distinct-" + step)
      }.persist (StorageLevel.MEMORY_ONLY_SER)
      val coarest_num=new_edges_str.map(s=>s._2._3).sum
      val t1_ge = System.nanoTime()
      println("old num:                        \t"+oldnum)
      //      println("oldedges_f num:                 \t"+ oldedges.map(s => (s._2._1._1.length)).sum().toLong)
      println("coarest num:                    \t"+coarest_num.toLong)
      println(s"generate new edges time:        \t ${((t1_ge - t0_ge)/1000000000.0).formatted("%.3f")} sec" )
      /**
        *2、 记录各分区情况
        */
      val par_INFO = new_edges_str.map(s=>s._2._2)
      deleteDir.deletedir(Param_df.islocal, Param_df.master, Param_df.output + "/par_INFO/step" + step)
      par_INFO.repartition(1).saveAsTextFile(Param_df.output + "/par_INFO/step" + step)
      var isnotBalance=false
      if(Param_df.output_Par_INFO){
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

        isnotBalance=(par_time_JOIN((par_time_JOIN.length * 0.75).toInt)*3 < par_time_JOIN(par_time_JOIN.length - 1)
          &&par_time_JOIN(par_time_JOIN.length-1)>30)
      }

      /**
        * 新边去重
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
        * Update Redis
        */
      val t0_hb = System.nanoTime(): Double
      deleteDir.deletedir(Param_df.islocal, Param_df.master, Param_df.hbase_output)
      redis_OP.Update(newedges)
      val t1_hb = System.nanoTime(): Double
      println("update Redis take time:         \t" + ((t1_hb - t0_hb) / 1000000000.0).formatted("%.3f") + " sec")

      new_edges_str.unpersist()

      /**
        * 更新旧边和新边
        */
      println("Start UNION")
      val t0_union = System.nanoTime()

      val cur_par=n_edges.getNumPartitions
      val need_par=(newnum/Param_df.newnum_interval)*Param_df.clusterpar

      val t0_new_flat_groupBykey=System.nanoTime()
      n_edges = {
        val origin=newedges.map(s=>(s(1),s(0))).groupByKey().mapValues(_.toArray)
        if(need_par>cur_par) origin.partitionBy(new HashPartitioner(need_par.toInt))
        else if(isnotBalance) origin.partitionBy(new HashPartitioner(cur_par.toInt))
        else origin
      }.persist(StorageLevel.MEMORY_ONLY_SER)

      if(step % Param_df.checkpoint_interval==0) {
        val t0_cp=System.nanoTime()
        n_edges.checkpoint()
//        println("checkpoint take time:           \t"+((System.nanoTime()-t0_cp)/1000000000.0).formatted("%.3f")
//          +" sec")
      }
      n_edges.count()

      println(s"n_edges time take time:          \t${((System.nanoTime() - t0_union)/1000000000.0).formatted("%" +
        ".3f")} sec")
      println("End UNION")

      println("*step: step " + step + " take time: \t " + ((System.nanoTime() - t0_turn) / 1000000000.0).formatted("%" +
        ".3f") + " sec")
      println
      continue = newnum != 0
      if(Param_df.check_edge)  scan.next()
    }


    println("final edges count():                     \t"+oldnum)
    println("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")
    print("@@  \t")
    println("Graspan End at "+df.format(System.currentTimeMillis())+"  \t@@")
    println("compute take time " +(System.nanoTime()-t0_all_2)/1000000000.0+ " sec")
    println("prepare take time "+time_prepare+" sec")
    println("Init Redis and Origin take time "+time_init_redis_origin/1000000000.0 +"sec")
    println("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")
    println()
//    println("Please confirm to end compute!")
//    scan.next()
    sc.stop()
  }

}


