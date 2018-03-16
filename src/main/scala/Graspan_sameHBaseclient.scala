///**
//  * Created by cycy on 2018/3/12.
//  */
//
//import java.util
//import java.util.Scanner
//import java.util.concurrent.Executors
//
//import org.apache.hadoop.conf.Configuration
//import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
//import org.apache.spark.rdd.RDD
//import org.apache.hadoop.mapreduce.Job
//import org.apache.hadoop.hbase._
//import org.apache.hadoop.hbase.client.{HBaseAdmin, HTable}
//import org.apache.hadoop.hbase.io.ImmutableBytesWritable
//import org.apache.hadoop.hbase.mapred.TableOutputFormat
//import org.apache.spark.storage.StorageLevel
//import utils._
//
//object Graspan_sameHBaseclient extends Para{
//
//  def computeInPartition_completely_flat_java_Array(step:Int,index:Int,
//                                                    mid_adj:Iterator[(VertexId,((Array[Array[Int]],Array[Array[Int]]),
//                                                      (Array[Array[Int]],Array[Array[Int]])))],
//                                                    symbol_num:Int,
//                                                    grammar:List[((EdgeLabel,EdgeLabel),EdgeLabel)],
//                                                    nodes_num_bitsize:Int,symbol_num_bitsize:Int,
//                                                    directadd:Map[EdgeLabel,EdgeLabel],
//                                                    is_complete_loop:Boolean,max_complete_loop_turn:Int,max_delta:Int,
//                                                    Batch_QueryHbase:Boolean,
//                                                    h_table:HTable,
//                                                    htable_split_Map:Map[Int,String],
//                                                    htable_nodes_interval:Int,
//                                                    queryHbase_interval:Int,
//                                                    default_split:String)
//  :Iterator[(Array[Array[Int]],List[String],Long)]={
//    var t0=System.nanoTime():Double
//    var t1=System.nanoTime():Double
//    var recording:List[String]=List()
//    var res_edges_array:Array[Array[Int]]=Array[Array[Int]]()
//    var coarest_num=0L
//    mid_adj.foreach(s=>{
//      val res=Graspan_OP_java.join_flat(s._1,
//        s._2._1._1//.toArray.map(x=>x.toArray)
//        ,s._2._1._2//.toArray.map(x=>x.toArray)
//        ,s._2._2._1//.toArray.map(x=>x.toArray)
//        ,s._2._2._2//.toArray.map(x=>x.toArray)
//        ,grammar.toArray.map(x=>Array(x._1._1,x._1._2,x._2)),symbol_num)
//      //      coarest_num += res.length
//      //      recording :+="*******************************"
//      //      recording :+="mid: "+s._1+"\n"
//      //      recording :+="old: "+s._2._1.map(x=>"("+"("+x._1+"),"+x._2+")").mkString(", ")+"\n"
//      //      recording :+="new: "+s._2._2.map(x=>"("+"("+x._1+"),"+x._2+")").mkString(", ")+"\n"
//      //      recording :+="res: "+res.toList.map(x=>"("+"("+x(0)+","+x(1)+"),"+x(2)+")").mkString(", ")+"\n"
//      //      recording :+="*******************************"
//      coarest_num +=res.size()
//      res_edges_array ++=res
//    })
//
//    //    var old_edges:List[(VertexId,VertexId,EdgeLabel)]=mid_adj_list.flatMap(s=>(s._2)).map(s=>(s._1._1,s._1._2,s._2))
//    println("At STEP "+step+", partition "+index)
//    recording:+="At STEP "+step+", partition "+index
//
//    val add_edges=res_edges_array.filter(s=>directadd.contains(s(2))).map(s=>(Array(s(0),s(1),directadd
//      .getOrElse(s(2),-1))))
//    res_edges_array=(res_edges_array ++ add_edges)
//    t1=System.nanoTime():Double
//    val toolong={
//      if((t1-t0) /1000000000.0<10) "normal"
//      else if((t1-t0) /1000000000.0 <100) "longer than 10"
//      else "longer than 100"
//    }
//    println()
//    println("||"
//      +" origin_formedges: "+coarest_num
//      +",\tadd_newedges: "+add_edges.length
//      +",\tdistinct newedges: " +res_edges_array.length+" ||"
//      +"join take time: "+toolong+", "+((t1-t0) /1000000000.0)+" secs")
//    recording :+=("|| "
//      +"origin_formedges: "+coarest_num
//      +",\tadd_newedges: "+add_edges.length
//      +",\tdistinct newedges: " +res_edges_array.length+" ||"
//      +"join take time: "+toolong+", "+((t1-t0) /1000000000.0)+" secs")
//    /**
//      * form clousure
//      * only focused on edges from key inpartition or to key inpartition
//      */
//    //    if(is_complete_loop){
//    //      val key_Set=mid_adj_list.map(s=>s._1).toSet
//    //      var continue:Boolean=is_complete_loop
//    //      var oldedges:List[(VertexId,VertexId,EdgeLabel,Boolean)]=old_edges.map(s=>(s._1,s._2,s._3,false))
//    //      var newedges:List[(VertexId,VertexId,EdgeLabel,Boolean)]=res_edges.map(s=>(s._1,s._2,s._3,true))
//    //      val first_new_num=newedges.length
//    //      val max_loop=max_complete_loop_turn
//    //      var turn=0
//    //      while(continue){
//    //        println("start loop ")
//    //        tmp_str+="start loop "
//    //        val t0=System.nanoTime():Double
//    //        turn+=1
//    //        val m_a_l=(oldedges ++ newedges).flatMap(s=>List((s._1,((s._1,s._2),s._3,s._4)),(s._2,((s._1,s._2),s._3,s._4)))
//    //        ).groupBy(_._1).toList.map(s=>(s._1,s._2.map(x=>x._2)))
//    //        val edges_before=(oldedges ++ newedges).map(s=>(s._1,s._2,s._3))
//    //        val (tmp_edges,tmp_str_inloop)=join(m_a_l,grammar,directadd)
//    //        tmp_str+=tmp_str_inloop
//    //        //      tmp_str+="bfore filter: "+tmp.length
//    //        oldedges=oldedges ++newedges.map(s=>(s._1,s._2,s._3,false))
//    //        //过滤新边，只保留与本partition有关的新边
//    //        newedges=tmp_edges.filter(s=>(key_Set.contains(s._1)||key_Set.contains(s._2))&&edges_before.contains(s)==false)
//    //          .map(s=>(s._1,s._2,s._3,true))
//    //        continue= (turn<max_loop && !newedges.isEmpty && oldedges.length-first_new_num<max_delta)
//    //        val t1=System.nanoTime():Double
//    //        println("complete_loop take time: "+((t1-t0)/ 1000000000.0/60).formatted("%.3f") + " min")
//    //        if(continue==false){
//    //          println("end loop")
//    //          tmp_str+="end loop"
//    //          recording:+=tmp_str
//    //          res_edges=oldedges.map(s=>(s._1,s._2,s._3))
//    //          println("after complete loop, res_edges= "+res_edges.length)
//    //          recording:+="after complete loop, res_edges= "+res_edges.length
//    //        }
//    //      }
//    //    }
//    /**
//      * 多线程开启
//      */
//    //    val executors = Executors.newCachedThreadPool()
//    //    val thread = new MyThread
//    //    class MyThread extends Thread{
//    //      override def run(): Unit = {
//    //
//    //      }
//    //    }
//    //    executors.submit(thread)
//    /**
//      * Hbase过滤
//      */
//    t0=System.nanoTime():Double
//    val len=res_edges_array.length
//    val res_edges= {
//      HBase_OP.queryHbase_inPartition_same_client(res_edges_array,nodes_num_bitsize,
//        symbol_num_bitsize,
//        Batch_QueryHbase,
//        h_table,
//        htable_split_Map,
//        htable_nodes_interval,
//        queryHbase_interval,default_split)
//    }
//    t1=System.nanoTime():Double
//    println("Query Hbase for edges: \t"+len
//      +",\ttake time: \t"+((t1-t0)/ 1000000000.0).formatted("%.3f") + " sec"
//      +", \tres_edges:             \t"+res_edges.length+"\n")
//    recording:+=("Query Hbase for edges: \t"+len
//      +",\ttake time: \t"+((t1-t0)/ 1000000000.0).formatted("%.3f") + " sec"
//      +", \tres_edges:             \t"+res_edges.length+"\n")
//    List((res_edges,recording,coarest_num)).toIterator
//    //    List((res_edges_array.toList,recording,coarest_num)).toIterator
//  }
//
//  def main(args: Array[String]): Unit = {
//    var t0=System.nanoTime():Double
//    var t1=System.nanoTime():Double
//    var islocal: Boolean = true
//    var master: String = "local"
//
//    var input_grammar: String = "data/test_grammar"
//    var input_graph:String="data/test_graph"
//    var output: String = "data/result/" //除去ip地址
//    var hbase_output:String="data/result/hbase/hbhfile/"
//    var defaultpar:Int=352
//    var smallpar:Int=96
//    var step_interval:Int=8
//    var newedges_interval:Int=40000000
//
//    var openBloomFilter:Boolean=false
//    var edges_totalnum:Int=1
//    var error_rate:Double=0.1
//
//    var htable_name:String="edges"
//    var queryHBase_interval:Int=50000
//    var HRegion_splitnum:Int=36
//    var Batch_QueryHbase:Boolean=true
//
//    var is_complete_loop:Boolean=false
//    var max_complete_loop_turn:Int=5
//    var max_delta:Int=10000
//
//
//    for (arg <- args) {
//      val argname = arg.split(",")(0)
//      val argvalue = arg.split(",")(1)
//      argname match {
//        case "islocal" => islocal = argvalue.toBoolean
//        case "master" => master = argvalue
//
//        case "input_grammar" => input_grammar = argvalue
//        case "input_graph"=>input_graph=argvalue
//        case "output" => output = argvalue
//        case "hbase_output"=>hbase_output=argvalue
//        case "smallpar"=>smallpar=argvalue.toInt
//        case "defaultpar"=>defaultpar=argvalue.toInt
//        case "newedges_interval"=>newedges_interval=argvalue.toInt
//
//        case "openBloomFilter"=>openBloomFilter=argvalue.toBoolean
//        case "edges_totalnum"=>edges_totalnum=argvalue.toInt
//        case "error_rate"=>error_rate=argvalue.toDouble
//
//        case "htable_name"=>htable_name=argvalue
//        case "queryHBase_interval"=>queryHBase_interval=argvalue.toInt
//        case "HRegion_splitnum"=>HRegion_splitnum=argvalue.toInt
//        case "Batch_QueryHbase"=>Batch_QueryHbase=argvalue.toBoolean
//
//        case "is_complete_loop"=>is_complete_loop=argvalue.toBoolean
//        case "max_complete_loop_turn"=>max_complete_loop_turn=argvalue.toInt
//        case "max_delta"=>max_delta=argvalue.toInt
//
//        case "step_interval"=>step_interval=argvalue.toInt
//        case _ => {}
//      }
//    }
//
//    /**
//      * 输出参数设置
//      */
//
//
//    /**
//      * Spark 设置
//      */
//    val conf = new SparkConf().set("spark.kryoserializer.buffer.max", "1024")
//    if (islocal) {
//      //test location can be adjusted or not
//      conf.setAppName("Graspan")
//      System.setProperty("hadoop.home.dir", "F:/hadoop-2.6.0/")
//      conf.setMaster("local")
//    }
//    val sc = new SparkContext(conf)
//    //    try {
//    println("------------Spark and HBase settings--------------------------------")
//    println("spark.driver.memory:          \t" + conf.get("spark.driver.memory"))
//    println("spark.executor.memory:        \t" + conf.get("spark.executor.memory"))
//    println("spark.executor.cores:         \t" + conf.get("spark.executor.cores"))
//    //      println("spark.storage.memoryFraction: \t" + conf.get("spark.storage.memoryFraction"))
//    //      println("spark.shuffle.memoryFraction: \t" + conf.get("spark.shuffle.memoryFraction"))
//    println("default partition num: \t" + defaultpar)
//    println("samll partition num:  \t" + smallpar)
//    println("queryHBase_interval:  \t" + queryHBase_interval)
//    println("HRegion_splitnum:     \t" + HRegion_splitnum)
//    println("--------------------------------------------------------------------")
//    println
//    /**
//      * Grammar相关设置
//      */
//    val grammar_origin = sc.textFile(input_grammar).map(s => s.split("\\s+")).collect().toList
//    val (symbol_Map, symbol_num, symbol_num_bitsize, loop, directadd, grammar) = Graspan_OP.processGrammar(grammar_origin,
//      input_grammar)
//    println("------------Grammar INFO--------------------------------------------")
//    println("input grammar:      \t" + input_grammar.split("/").last)
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
//    grammar.foreach(s => println("                    \t" + s._1._1 + "\t+\t" + s._1._2 + "\t->\t" + s._2))
//    println("---------------------------------------------------------------------")
//    println
//
//    /**
//      * Graph相关设置
//      */
//    val (graph, nodes_num_bitsize, nodes_totalnum) = Graspan_OP.processGraph(sc, input_graph, input_grammar, symbol_Map, loop,
//      directadd, defaultpar)
//
//    println("------------Graph INFO--------------------------------------------")
//    println("input graph:        \t" + input_graph.split("/").last)
//    println("processed edges:    \t" + graph.count())
//    println("nodes_totoalnum:    \t" + nodes_totalnum)
//    println("nodes_num_bitsize:  \t" + nodes_num_bitsize)
//    println("------------------------------------------------------------------")
//    println
//
//    val (htable_split_Map, default_split) = HBase_OP.createHBase_Table(htable_name, HRegion_splitnum)
//
//    /**
//      * 原边集存入Hbase
//      */
//    //    println("graph Partitions: "+graph.partitions.length)
//    deleteDir.deletedir(islocal, master, hbase_output)
//    HBase_OP.updateHbase(graph, nodes_num_bitsize, symbol_num_bitsize, htable_name, hbase_output,
//      htable_split_Map, HRegion_splitnum, default_split)
//
//    /**
//      * HBase 连接建立
//      */
//    val h_conf = HBaseConfiguration.create()
//    h_conf.set(TableOutputFormat.OUTPUT_TABLE, htable_name)
//    h_conf.set("hbase.zookeeper.quorum", "slave001,slave002,slave003")
//    h_conf.setInt("hbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily", 4096)
//    val h_table =new HTable(h_conf, htable_name)
//
//    deleteDir.deletedir(islocal, master, output)
//
//    var oldedges: RDD[(VertexId, ((Array[Array[Int]],Array[Array[Int]]),(Array[Array[Int]],Array[Array[Int]])))] =
//      graph.flatMap(s => Array((s._1,Array(Array(s._1,s._2,s._3))), (s._2, Array(Array(s._1,s._2,s._3)))))
//        .reduceByKey((x,y)=>(x ++ y))
//        .repartition(defaultpar)
//        .mapPartitions(s=>{
//          s.map(u=>{
//            val flag=u._1
//            (u._1,((Array[Array[Int]](),Array[Array[Int]]()),(u._2.filter(x=>x(1)==flag).map(x=>Array(x(2),x(0))),
//              u._2.filter(x=>x(0)==flag).map(x=>Array(x(2),x(1))))))
//          })
//        })
//        .persist(StorageLevel.MEMORY_ONLY_SER)
//
//    var step = 0
//    var change_par=true
//    var continue: Boolean = true
//    var newnum: Long = graph.count()
//    /**
//      * 开始迭代
//      */
//    while (continue) {
//      t0 = System.nanoTime(): Double
//      step += 1
//      println("\n************During step " + step + "************")
//      /**
//        * 计算
//        */
//      println("current partitions num:         \t"+oldedges.getNumPartitions)
//      val new_edges_str = oldedges
//        .mapPartitionsWithIndex((index, s) =>
//          computeInPartition_completely_flat_java_Array(step,
//            index, s,
//            symbol_num,grammar,
//            nodes_num_bitsize,
//            symbol_num_bitsize, directadd, is_complete_loop, max_complete_loop_turn, max_delta, Batch_QueryHbase,h_table,
//            htable_split_Map,
//            HRegion_splitnum, queryHBase_interval, default_split)).persist(StorageLevel.MEMORY_ONLY_SER)
//      /**
//        * 记录各分区情况
//        */
//      val par_INFO = new_edges_str.map(s=>s._2)
//      deleteDir.deletedir(islocal, master, output + "/par_INFO/step" + step)
//      par_INFO.repartition(1).saveAsTextFile(output + "/par_INFO/step" + step)
//      val coarest_num=new_edges_str.map(s=>s._3).sum
//      println("coarest_num:                    \t"+coarest_num.toLong)
//      /**
//        * 新边去重
//        */
//      val t0_distinct=System.nanoTime():Double
//      val newedges = new_edges_str.flatMap(s=>s._1).mapPartitions(s=>s.map(_.toVector)).distinct
//        .mapPartitions(s=>s.map(_.toArray)).persist(StorageLevel.MEMORY_ONLY_SER)
//      newnum = newedges.count()
//      println("newedges:                       \t" + newnum)
//      println("distinct take time:             \t" + ((System.nanoTime()-t0_distinct)/1000000000.0).formatted("%" +
//        ".3f")+" secs")
//      println("compute take time:              \t" + ((System.nanoTime()-t0)/1000000000.0).formatted("%.3f")+" secs")
//      new_edges_str.unpersist()
//
//      /**
//        * 更新旧边和新边
//        */
//
//      oldedges = {
//        if(newnum>=100000&&step<=40&&step%step_interval==1&&change_par==false){
//          change_par=true
//          (oldedges cogroup
//            newedges.flatMap(s=>(Array((s(0),Array(s)),(s(1),Array(s))))).reduceByKey(_ ++ _) )
//            .mapPartitions(s =>{
//              s.map(x=>{
//                val flag=x._1
//                val old_part=x._2._1.headOption.getOrElse(((Array[Array[Int]](),Array[Array[Int]]()),(Array[Array[Int]]
//                  (),Array[Array[Int]]())))
//                val new_part=x._2._2.headOption.getOrElse(Array[Array[Int]]())
//                (flag,((old_part._1._1 ++ old_part._2._1,old_part._1._2 ++ old_part._2._2),(new_part.filter(u=>u(1)
//                  ==flag).map(u=>Array(u(2),u(0))),new_part.filter(u=>u(0)==flag).map(u=>Array(u(2),u(1))))))
//              })
//            })
//            .repartition((step/step_interval+1)*defaultpar)
//            .persist(StorageLevel.MEMORY_ONLY_SER)
//        }
//        else{
//          change_par=false
//          (oldedges cogroup
//            newedges.flatMap(s=>(Array((s(0),Array(s)),(s(1),Array(s))))).reduceByKey(_ ++ _) )
//            .mapPartitions(s =>{
//              s.map(x=>{
//                val flag=x._1
//                val old_part=x._2._1.headOption.getOrElse(((Array[Array[Int]](),Array[Array[Int]]()),(Array[Array[Int]]
//                  (),Array[Array[Int]]())))
//                val new_part=x._2._2.headOption.getOrElse(Array[Array[Int]]())
//                (flag,((old_part._1._1 ++ old_part._2._1,old_part._1._2 ++ old_part._2._2),(new_part.filter(u=>u(1)
//                  ==flag).map(u=>Array(u(2),u(0))),new_part.filter(u=>u(0)==flag).map(u=>Array(u(2),u(1))))))
//              })
//            })
//            //            .repartition(smallpar)
//            .persist(StorageLevel.MEMORY_ONLY_SER)
//        }
//      }
//
//
//      /**
//        * Update HBase
//        */
//      val t0_hb = System.nanoTime(): Double
//      deleteDir.deletedir(islocal, master, hbase_output)
//      HBase_OP.updateHbase_same_client(newedges, nodes_num_bitsize, symbol_num_bitsize,
//        h_conf.asInstanceOf[HBaseConfiguration], h_table,
//        hbase_output,
//        htable_split_Map, HRegion_splitnum, default_split)
//      val t1_hb = System.nanoTime(): Double
//      println("update Hbase take time:         \t" + ((t1_hb - t0_hb) / 1000000000.0).formatted("%.3f") + " sec")
//      t1 = System.nanoTime(): Double
//      println("*step: step " + step + " take time: \t " + ((t1 - t0) / 1000000000.0).formatted("%.3f") + " sec")
//      println
//      continue = newnum != 0
//      //      val scan = new Scanner(System.in)
//      //      scan.next()
//
//    }
//    println("final edges count:             \t" + oldedges.mapValues(s => (s._1._1.length)).reduce((x,y)=>((1,x._2+y
//      ._2)))._2)
//    sc.stop()
//  }
//
//}
//
//
