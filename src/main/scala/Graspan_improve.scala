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
import utils.{Graspan_OP, HBase_OP, Para, deleteDir}

import scala.collection.mutable.ArrayBuffer

object Graspan_improve extends Para{

  def check_edge_RDD(edges:RDD[(Int,(Array[Int],Int,Int,Int))]):RDD[String]={
    edges.map(s=>{
      val len=s._2._1.length
      val old_f_end=s._2._2
      val new_f_end=s._2._3
      val old_b_end=s._2._4
      if(old_f_end<=new_f_end && new_f_end<=old_b_end && old_b_end<len && len%2==0 && old_f_end%2!=0 && new_f_end%2!=0
        && old_b_end%2!=0 ) "OK"
      else "edges: "+len+","+old_f_end+","+new_f_end+","+old_b_end+
        s"  is 4 part index error: ${old_f_end<=new_f_end && new_f_end<=old_b_end && old_b_end<len}"+
      s" is fill array error: ${len%2==0 && old_f_end%2!=0 && new_f_end%2!=0 && old_b_end%2!=0}"
    })
  }
  def main(args: Array[String]): Unit = {
    val scan = new Scanner(System.in)

    var t0=System.nanoTime():Double
    var t1=System.nanoTime():Double
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

    var htable_name:String="edges"
    var queryHBase_interval:Int=50000
    var HRegion_splitnum:Int=36
    var Batch_QueryHbase:Boolean=true

    var is_complete_loop:Boolean=false
    var max_complete_loop_turn:Int=5
    var max_delta:Int=10000

    var file_index_f:Int= -1
    var file_index_b:Int= -1

    var check_edge:Boolean=false

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

        case "htable_name"=>htable_name=argvalue
        case "queryHBase_interval"=>queryHBase_interval=argvalue.toInt
        case "HRegion_splitnum"=>HRegion_splitnum=argvalue.toInt
        case "Batch_QueryHbase"=>Batch_QueryHbase=argvalue.toBoolean

        case "is_complete_loop"=>is_complete_loop=argvalue.toBoolean
        case "max_complete_loop_turn"=>max_complete_loop_turn=argvalue.toInt
        case "max_delta"=>max_delta=argvalue.toInt

        case "newnum_interval"=>newnum_interval=argvalue.toInt
        case "checkpoint_interval"=>checkpoint_interval=argvalue.toInt
        case "checkpoint_output"=>checkpoint_output=argvalue
        case "file_index_f"=>file_index_f=argvalue.toInt
        case "file_index_b"=>file_index_b=argvalue.toInt

        case "check_edge"=>check_edge=argvalue.toBoolean
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
    val grammar_origin = sc.textFile(input_grammar).map(s => s.split("\\s+")).collect().toList
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

    val (htable_split_Map, default_split) = HBase_OP.createHBase_Table(htable_name, HRegion_splitnum)

    /**
      * 原边集存入Hbase
      */
    //    println("graph Partitions: "+graph.partitions.length)
    deleteDir.deletedir(islocal, master, hbase_output)
    HBase_OP.updateHbase(graph, nodes_num_bitsize, symbol_num_bitsize, htable_name, hbase_output,
      htable_split_Map, HRegion_splitnum, default_split)
//    scan.next()
    deleteDir.deletedir(islocal, master, output)

    var oldedges: RDD[(VertexId, (Array[Int],Int,Int,Int))] =
      graph.flatMap(s => {
        if(s._1!=s._2) Array((s._1,Array(Array(s._1,s._2,s._3))), (s._2, Array(Array(s._1,s._2,s._3))))
        else Array((s._1,Array(Array(s._1,s._2,s._3))))
      })
        .reduceByKey((x,y)=>(x ++ y))
        .partitionBy(new HashPartitioner(defaultpar))
        .mapPartitions(s=>{
          s.map(u=>{
            val flag=u._1
            val new_f=new ArrayBuffer[Int](u._2.length*2)
            val new_b=new ArrayBuffer[Int](u._2.length*2)
            var index=0
            while(index<u._2.length){
              val ele=u._2(index)
              if(ele(1)==flag){
                new_f.append(ele(2))
                new_f.append(ele(0))
              }
              if(ele(0)==flag){
                new_b.append(ele(2))
                new_b.append(ele(1))
              }
              index+=1
            }
            val index_new_f_end={
              if(new_f.length>0) new_f.length-1
              else -1
            }
            new_f.appendAll(new_b)
            (u._1,(new_f.toArray,-1,index_new_f_end,index_new_f_end))
          })
        })
        .partitionBy(new HashPartitioner(defaultpar))
        .persist(StorageLevel.MEMORY_ONLY_SER)
    //    oldedges.count()
    println(s"oldedges.partitioner:${oldedges.partitioner}")

//    println("check oldedges")
//    println(check_edge_RDD(oldedges).filter(s=> !s.contains("OK")).top(10).mkString("\n"))
    var step = 0
    var change_par=true
    var continue: Boolean = true
    var newnum: Long = graph.count()
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
      println("current partitions num:         \t"+oldedges.getNumPartitions)

      val t0_ge = System.nanoTime()
      val new_edges_str = oldedges
        .mapPartitionsWithIndex((index, s) =>
          Graspan_OP.computeInPartition_fully_compressed(step,
            index, s,
            symbol_num,grammar,
            nodes_num_bitsize,
            symbol_num_bitsize, directadd, is_complete_loop, max_complete_loop_turn, max_delta,
            Batch_QueryHbase,
            htable_name,
            htable_split_Map,
            HRegion_splitnum, queryHBase_interval, default_split)).setName("newedge-before-distinct-" + step)
        .persist (StorageLevel
          .MEMORY_ONLY_SER)
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

      val cur_par=oldedges.getNumPartitions
      val oldEdgesPartitioner = oldedges.partitioner.getOrElse(new HashPartitioner(cur_par))
      val need_par=(newnum/newnum_interval)*clusterpar

      val t0_new_flat_groupBykey=System.nanoTime()
      val newedges_to_be_cogrouped = newedges.flatMap(s => {
        if(s(0)!=s(1)) Array((s(0),s),(s(1),s))
        else Array((s(0),s))
      }).groupByKey(oldEdgesPartitioner).mapValues(s=>s.toArray)
      newedges_to_be_cogrouped.count
      println("newedges_to_be_cogrouped:              \t"+((System.nanoTime()-t0_new_flat_groupBykey)/1000000000.0).formatted("%.3f")+
        "sec")

//      val accum_time_old_map=sc.longAccumulator("map")
//      val accum_nodes=sc.longAccumulator("nodes")
//      val accum_old=sc.longAccumulator("old count")
//      val accum_new=sc.longAccumulator("new count")
//      val accum_time_old=sc.longAccumulator("old time")
//      val accum_time_new=sc.longAccumulator("new time")
//      var t0_old_becomearray=System.nanoTime()
//      val accum_old_becomearray=sc.longAccumulator("old_becomearray")
      oldedges = {
        val t0_old_cogroup=System.nanoTime()
        val oldedges_cogroup=({
          //            if (Some(oldEdgesPartitioner) == oldedges.partitioner && Some(oldEdgesPartitioner) ==
          //              newedges_to_be_cogrouped.partitioner) {
          //              println("Joining old and new edges with co-partition shuffle!")
          //            }
          //            println(s"oldEdgesPartitioner:${oldEdgesPartitioner}")
          //            println(s"oldedges.partitioner:${oldedges.partitioner}")
          //            println(s"newedges partitioner:${newedges_to_be_cogrouped.partitioner}")
          //            println(s"equal?${Some(oldEdgesPartitioner) == oldedges.partitioner}|${Some(oldEdgesPartitioner) ==
          //              newedges_to_be_cogrouped.partitioner}")
          oldedges.cogroup(newedges_to_be_cogrouped,oldEdgesPartitioner).setName("cogrouped-"+step).cache()
        })
        oldedges_cogroup.count
        println("oldedges_cogroup take time:            \t"+((System.nanoTime()-t0_old_cogroup)/1000000000.0).formatted("%.3f")+" sec")
        if(need_par>cur_par){
          println("edges num is increasing, add Tasks")
          oldedges_cogroup.mapPartitions(v=>{
            val t0_old_map_array=System.nanoTime()
            v.map(x=>{
              val flag = x._1
              if(!x._2._1.isEmpty&&x._2._2.isEmpty){//没新边，只有旧边，只需要修改指针
                (flag,(x._2._1.head._1,x._2._1.head._3,x._2._1.head._3,x._2._1.head._1.length-1))
              }
              else if(x._2._1.isEmpty&& ! x._2._2.isEmpty){//没旧边，只有新边
              val new_part=x._2._2.head
                val new_f=new ArrayBuffer[Int](new_part.length*2)
                val new_b=new ArrayBuffer[Int](new_part.length*2)
                var index=0
                while(index<new_part.length){
                  val ele=new_part(index)
                  if(ele(1)==flag){
                    new_f.append(ele(2))
                    new_f.append(ele(0))
                  }
                  if(ele(0)==flag){
                    new_b.append(ele(2))
                    new_b.append(ele(1))
                  }
                  index+=1
                }
                val index_new_f_end={
                  if(new_f.length>0) new_f.length-1
                  else -1
                }
                new_f.appendAll(new_b)
                (flag,(new_f.toArray,-1,index_new_f_end,index_new_f_end))
              }
              else{//新旧都有
              val old_part=x._2._1.head
                val new_part=x._2._2.head
                val new_f=new ArrayBuffer[Int](new_part.length*2)
                val new_b=new ArrayBuffer[Int](new_part.length*2)
                var index=0
                while(index<new_part.length){
                  val ele=new_part(index)
                  if(ele(1)==flag){
                    new_f.append(ele(2))
                    new_f.append(ele(0))
                  }
                  if(ele(0)==flag){
                    new_b.append(ele(2))
                    new_b.append(ele(1))
                  }
                  index+=1
                }
                val new_f_array:Array[Int]=new_f.toArray
                val new_b_array:Array[Int]=new_b.toArray
                val index_old_f_end=old_part._3
                val index_new_f_end=index_old_f_end+new_f_array.length
                val index_old_b_end=old_part._1.length-old_part._3-1+index_new_f_end
                val array:Array[Int]=old_part._1.slice(0,old_part._3+1) ++ new_f_array ++
                  old_part._1.slice(old_part._3+1,old_part._1.length) ++ new_b_array
                if(array.length%2 !=0) System.exit(0)
                (flag,(array,
                  index_old_f_end,
                  index_new_f_end,
                  index_old_b_end))
              }
            })
          })
            .partitionBy(new HashPartitioner(need_par.toInt))
        }
        else if(par_time_JOIN((par_time_JOIN.length * 0.75).toInt)*3 < par_time_JOIN(par_time_JOIN.length - 1)
          &&par_time_JOIN(par_time_JOIN.length-1)>30){
          println("Not Balance,repar")
          oldedges_cogroup.mapPartitions(v=>{
            val t0_old_map_array=System.nanoTime()
            v.map(x=>{
              val flag = x._1
              if(!x._2._1.isEmpty&&x._2._2.isEmpty){//没新边，只有旧边，只需要修改指针
                (flag,(x._2._1.head._1,x._2._1.head._3,x._2._1.head._3,x._2._1.head._1.length-1))
              }
              else if(x._2._1.isEmpty&& ! x._2._2.isEmpty){//没旧边，只有新边
              val new_part=x._2._2.head
                val new_f=new ArrayBuffer[Int](new_part.length*2)
                val new_b=new ArrayBuffer[Int](new_part.length*2)
                var index=0
                while(index<new_part.length){
                  val ele=new_part(index)
                  if(ele(1)==flag){
                    new_f.append(ele(2))
                    new_f.append(ele(0))
                  }
                  if(ele(0)==flag){
                    new_b.append(ele(2))
                    new_b.append(ele(1))
                  }
                  index+=1
                }
                val index_new_f_end={
                  if(new_f.length>0) new_f.length-1
                  else -1
                }
                new_f.appendAll(new_b)
                (flag,(new_f.toArray,-1,index_new_f_end,index_new_f_end))
              }
              else{//新旧都有
              val old_part=x._2._1.head
                val new_part=x._2._2.head
                val new_f=new ArrayBuffer[Int](new_part.length*2)
                val new_b=new ArrayBuffer[Int](new_part.length*2)
                var index=0
                while(index<new_part.length){
                  val ele=new_part(index)
                  if(ele(1)==flag){
                    new_f.append(ele(2))
                    new_f.append(ele(0))
                  }
                  if(ele(0)==flag){
                    new_b.append(ele(2))
                    new_b.append(ele(1))
                  }
                  index+=1
                }
                val new_f_array:Array[Int]=new_f.toArray
                val new_b_array:Array[Int]=new_b.toArray
                val index_old_f_end=old_part._3
                val index_new_f_end=index_old_f_end+new_f_array.length
                val index_old_b_end=old_part._1.length-old_part._3-1+index_new_f_end
                val array:Array[Int]=old_part._1.slice(0,old_part._3+1) ++ new_f_array ++
                  old_part._1.slice(old_part._3+1,old_part._1.length) ++ new_b_array
                if(array.length%2 !=0) System.exit(0)
                (flag,(array,
                  index_old_f_end,
                  index_new_f_end,
                  index_old_b_end))
              }
            })
          })
            .partitionBy(new HashPartitioner(cur_par))
        }
        else{
          oldedges_cogroup.mapPartitions(v=>{
            val t0_old_map_array=System.nanoTime()
            v.map(x=>{
              val flag = x._1
              if(!x._2._1.isEmpty&&x._2._2.isEmpty){//没新边，只有旧边，只需要修改指针
                (flag,(x._2._1.head._1,x._2._1.head._3,x._2._1.head._3,x._2._1.head._1.length-1))
              }
              else if(x._2._1.isEmpty&& ! x._2._2.isEmpty){//没旧边，只有新边
                val new_part=x._2._2.head
                val new_f=new ArrayBuffer[Int](new_part.length*2)
                val new_b=new ArrayBuffer[Int](new_part.length*2)
                var index=0
                while(index<new_part.length){
                  val ele=new_part(index)
                  if(ele(1)==flag){
                    new_f.append(ele(2))
                    new_f.append(ele(0))
                  }
                  if(ele(0)==flag){
                    new_b.append(ele(2))
                    new_b.append(ele(1))
                  }
                  index+=1
                }
                val index_new_f_end={
                  if(new_f.length>0) new_f.length-1
                  else -1
                }
                new_f.appendAll(new_b)
                (flag,(new_f.toArray,-1,index_new_f_end,index_new_f_end))
              }
              else{//新旧都有
                val old_part=x._2._1.head
                val new_part=x._2._2.head
                val new_f=new ArrayBuffer[Int](new_part.length*2)
                val new_b=new ArrayBuffer[Int](new_part.length*2)
                var index=0
                while(index<new_part.length){
                  val ele=new_part(index)
                  if(ele(1)==flag){
                    new_f.append(ele(2))
                    new_f.append(ele(0))
                  }
                  if(ele(0)==flag){
                    new_b.append(ele(2))
                    new_b.append(ele(1))
                  }
                  index+=1
                }
                val new_f_array:Array[Int]=new_f.toArray
                val new_b_array:Array[Int]=new_b.toArray
                val index_old_f_end=old_part._3
                val index_new_f_end=index_old_f_end+new_f_array.length
                val index_old_b_end=old_part._1.length-old_part._3-1+index_new_f_end
                val array:Array[Int]=old_part._1.slice(0,old_part._3+1) ++ new_f_array ++
                  old_part._1.slice(old_part._3+1,old_part._1.length) ++ new_b_array
                if(array.length%2 !=0) System.exit(0)
                (flag,(array,
                  index_old_f_end,
                  index_new_f_end,
                  index_old_b_end))
              }
            })
          })
      }
    }.setName("unioned-edges-" + step).persist(StorageLevel.MEMORY_ONLY_SER)
//      accum_old.value
//      accum_new.value
//      accum_time_old.value
//      accum_time_new.value
//      accum_old_becomearray.value

    if(step % checkpoint_interval==0) {
      val t0_cp=System.nanoTime()
      oldedges.checkpoint()
      println("newedges:                       \t")
      println("checkpoint take time:           \t"+((System.nanoTime()-t0_cp)/1000000000.0).formatted("%.3f")
        +" sec")
    }
      oldedges.count()

    println(s"union time take time:            \t${((System.nanoTime() - t0_union)/1000000000.0).formatted("%.3f")} sec")
      println("End UNION")

    t1 = System.nanoTime()
    println("*step: step " + step + " take time: \t " + ((t1 - t0) / 1000000000.0).formatted("%.3f") + " sec")
    println
    continue = newnum != 0

      if(check_edge){
              println("check oldedges")
              println(check_edge_RDD(oldedges).filter(s=> !s.contains("OK")).top(100).mkString("\n"))
//        scan.next()
      }


  }

  sc.stop()
  println("final edges count():                     \t"+oldnum)
}

}


