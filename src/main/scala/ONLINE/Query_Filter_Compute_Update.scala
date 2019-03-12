package ONLINE

import java.io.File
import java.util
import java.util.Scanner
import javax.management

import ONLINE.ProtocolBuffer.ProtocolBuffer_OP
import ONLINE.ProtocolBuffer.ProtocolBuffer_OP._
import ONLINE.utils_ONLINE.{BIgSpa_OP, Param_pt, Redis_OP}
import cn.edu.nju.pasalab.db.ShardedRedisClusterClient
import it.unimi.dsi.fastutil.ints.IntOpenHashSet
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.storage.StorageLevel

import collection.JavaConversions
import scala.collection.mutable.ArrayBuffer
import scala.io.Source
/**
  * Created by cycy on 2019/3/5.
  */
object Query_Filter_Compute_Update {
  var NodeSet:IntOpenHashSet=new IntOpenHashSet()
  /**
    * Encode
    */
  def EncodeLabel_pos(label:Int,pos:Int):Int=(label<<1)+pos
  def EncodeVid_label_pos(vid:Int,label:Int,pos:Int):Long= (vid.toLong << 32)+(label<<1)+pos
  def EncodeVid_labelpos(vid:Int,labelpos:Int):Long=(vid.toLong<<32)+labelpos
  def EncodeCount(count:Int,dir:Int):Long={
    if(dir==0) count.toLong <<32
    else count.toLong
  }
  /**
    * Decode
    */
  def DecodeLabel_pos(label_pos:Int):(Int,Int)=((label_pos>>1),label_pos&0x1)
  def DecodeVid_label_pos(vid_label_pos:Long):(Int,Int,Int)={
    //无符号右移
    val label_pos=vid_label_pos&0xffffl
    val (label,pos)=DecodeLabel_pos(label_pos.toInt)
    ((vid_label_pos>>>32).toInt,label,pos)
  }
  def DecodeVid_labelpos(vid_label_pos:Long):(Int,Int)=((vid_label_pos>>>32).toInt,(vid_label_pos&0xffffl).toInt)

  def DecodeCounts(counts:Long):(Int,Int)={
    ((counts>>>32).toInt,(counts & 0xffffl).toInt)
  }

  //Map[label_f/label_b,Array[(label_b/label_f,label_target)]
  def parseGrammar(grammar:Array[Array[Int]]):Map[Int,Array[(Int,Int)]]={
    grammar.flatMap(g=>{
      val (label_f,label_b,label_t)=(g(0),g(1),g(2))
      Array((EncodeLabel_pos(label_f,0),(EncodeLabel_pos(label_b,1),label_t)),(EncodeLabel_pos(label_b,1),
        (EncodeLabel_pos(label_f,0),label_t)))
    }).groupBy(_._1).map(l=>(l._1,l._2.map(_._2)))
  }

  /**
    * 部分Redis操作移到此处，以避免Redis转换时的重复空间分配
    */
  def Long2ByteArray(l:Long):Array[Byte]={
    val res=new Array[Byte](8)
    for(i<-0 to 7)
      res(i)=((l>>(8*(7-i)))&0xff).toByte
    res
  }

  /**
    * Main
    * @param args
    */
  def main(args: Array[String]): Unit = {
    var t0=System.nanoTime()
    var t1=System.nanoTime()
    val scan=new Scanner(System.in)
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

    val grammar_match:Map[Int,Array[(Int,Int)]]=parseGrammar(grammar)
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
    println()
    println("grammar_match:      \t")
    grammar_match.foreach(s => println("                    \t" + s._1 + "\t:\t" + s._2.mkString(",")))
    println("---------------------------------------------------------------------")
    println

    val Symbol_Map_read:Map[Int,String]=symbol_Map.map(s=>(s._2,s._1))
    /**
      * 设置Redis
      */
    ShardedRedisClusterClient.getProcessLevelClient.clearDB()
    val redis_OP = new Redis_OP(Param_pt.updateRedis_interval)

    /**
      * RDD读入原边集
      */

    println("test map protocol buffer at driver")
    val map:java.util.Map[Integer,java.lang.Long]=new util.HashMap[Integer,java.lang.Long]()
    map.put(1,2l)
    map.put(2,4l)
    val str=Serialzed_Map_UidCounts(map)
    println(new String(str))
    //    scan.next()

    if(Param_pt.input_graph!="null") {
      t0 = System.nanoTime()
      val original_edges: RDD[(Long, java.util.Map[Integer, java.lang.Long])] = sc.textFile(Param_pt.input_graph, Param_pt
        .clusterpar).map(line => {
        val parts = line.split(":")
        val part2_split = parts(1).split("\\s+")
        val len = part2_split.length
        if (len % 2 != 0) println("error")
        val javaMap = new util.HashMap[Integer, java.lang.Long]()
        Array.range(0, len / 2).map(index => javaMap.put(part2_split(index * 2).toInt, part2_split(index * 2 + 1).toLong))
        (parts(0).toLong, javaMap)
      })
      t1 = System.nanoTime()
      println("original_edges first: " + original_edges.first())
      NodeSet = new IntOpenHashSet(original_edges.map(s => DecodeVid_labelpos(s._1)._1).collect())
      println("old nums: "+NodeSet.size())
      println("get original_edges use " + (t1 - t0) / 1e9)

      t0 = System.nanoTime()
      redis_OP.Update_PB(original_edges)
      t1 = System.nanoTime()
      println("put original_edges into redis " + (t1 - t0) / 1e9)

      original_edges.unpersist()
    }

    /**
      * Streaming
      */

    while(true) {
      println("please input the added edges file path")

      val source: Array[String] = {
//        var path = scan.nextLine()
//        var file=new File(path)
//        while(!file.exists()){
//          path=scan.nextLine()
//          file=new File(path)
//        }
//        Source.fromFile(path).getLines().toArray
        Source.fromFile(Param_pt.add).getLines().toArray
      }
      //source 的数量保证小批量
      val t0_delta=System.nanoTime()
      var add_edges: Array[(Int, Int, Int)] = source.flatMap(line => {
        val s = line.split("\\s+")
        val res = new ArrayBuffer[(Int, Int, Int)](3)
        val (src, dst, label) = (s(0).toInt, s(1).toInt, symbol_Map.getOrElse(s(2), -1))
        if (label == -1) println("there is some label wrong in input")
        res.append((src, dst, label))
        if (NodeSet.contains(src) == false) {
          for (l <- loop) res.append((src, src, l))
          NodeSet.add(src)
        }
        if (NodeSet.contains(dst) == false) {
          for (l <- loop) res.append((dst, dst, l))
          NodeSet.add(dst)
        }
        res
      })
      println("add_edges_count: "+add_edges.length)
      //      add_edges.foreach(s => println(s._1, s._2, Symbol_Map_read(s._3)))
      var add_edges_rdd: RDD[(Int, Int, Int)] = null

      var step = 0 //为0表示直接添加，>0表示三角添加
      var add_edges_len = add_edges.length
      while (add_edges_len > 0) {
        val t0_1=System.nanoTime()
        if (add_edges_len < Param_pt.changemode_interval) {
          println("@@@@@@@@@@@@@  SingleMachine @@@@@@@@@@@@@@@@@@@@@")
          if (add_edges == null) {
            add_edges = add_edges_rdd.collect()
            add_edges_rdd.unpersist()
            add_edges_rdd = null
          }
          add_edges = ComputeSingleMachine(add_edges, grammar_match, loop, directadd, step, redis_OP)
          add_edges_len = add_edges.length
          println("add_edges count: "+add_edges.length)
          //          add_edges.foreach(s => println(s._1, s._2, Symbol_Map_read(s._3)))
        }
        else {
          println("@@@@@@@@@@@@@  Distributed  @@@@@@@@@@@@@")
          if (add_edges_rdd == null) {
            add_edges_rdd = sc.parallelize(add_edges, Param_pt.clusterpar)
            add_edges = null
          }
          add_edges_rdd = ComputeDistribution(add_edges_rdd, grammar_match, loop, directadd, step, redis_OP)
            .persist(StorageLevel.MEMORY_ONLY_SER)
          add_edges_len = add_edges_rdd.count().toInt
          println("add_edges count: "+add_edges_rdd.count())
        }
        val t1_1=System.nanoTime()
        println(s" \t step $step uses " + (t1_1-t0_1)/1e9 )
        step += 1
        val pause=scan.nextLine()
      }
      val t1_delta=System.nanoTime()
      println("update use "+(t1_delta-t0_delta)/1e9)
    }

  }

  def ComputeSingleMachine(add_edges:Array[(Int,Int,Int)],grammar_match:Map[Int,Array[(Int,Int)]],loop:List[Int],
                           directadd:Map[Int,Int],
                           step:Int,
                           redis_OP:Redis_OP):Array[(Int,Int,Int)]={
    /**
      * 1 统计数据
      */
    val t0_1=System.nanoTime()
    var add_edges_count:Array[((Int,Int,Int),Int)]=add_edges.map(e=>(e,1)).groupBy(_._1).map(eg=>(eg._1,eg._2.length)).toArray
    println("add_edges_count: "+add_edges_count.length)//+" \n"+add_edges_count.mkString("\n")
    val distinct_newedges_count=add_edges_count.length
    val t1_1=System.nanoTime()
    println(" 1: form add_edges_count uses \t"+(t1_1-t0_1)/1e9)
    /**
      * 2 计算所需查询的所有key，包括
      *   1) Update_Count所需的key : src_label_b and dst_label_f ，其中任选其一可作为filter
      *   2) Compute new edges 所需的key : [src_x_f] and [dst_y_b]
      *
      *   数据格式: (
      *     (src,dst,label),
      *     (src_label_,dst_label_f),
      *     Array[Arraybuffer[vid_label_pos,label_target]] : [(src_x_f,A)] and [(dst_y_b,B)]
      *     )
      */
    //
    val t0_2=System.nanoTime()
    val newedges_count_update_compute:Array[(((Int,Int,Int),Int),(Long,Long),Array[ArrayBuffer[(Long,Int)]])]=
      add_edges_count.map(ec=>{
        val (src,dst,label)=ec._1
        val update_src=EncodeVid_label_pos(src,label,1)
        val update_dst=EncodeVid_label_pos(dst,label,0)
        val compute=new Array[ArrayBuffer[(Long,Int)]](2)
        for(i<-0 to 1)
          compute(i)=new ArrayBuffer[(Long, Int)]()
        grammar_match.getOrElse(EncodeLabel_pos(label,1),Array[(Int,Int)]())
          .foreach(s=> {
            val (x_f, label_A) = s
            compute(0).append((EncodeVid_labelpos(src, x_f), label_A))
          })
        grammar_match.getOrElse(EncodeLabel_pos(label,0),Array[(Int,Int)]()).foreach(s=>{
          val (y_b,label_B)=s
          compute(1).append((EncodeVid_labelpos(dst,y_b),label_B))
        })
        (ec,(update_src,update_dst),compute)
      })
    val t1_2=System.nanoTime()
    println(" 2: compute all Query keys  uses \t"+(t1_2-t0_2)/1e9)
//    println("newedges_count_update_compute: \n"+newedges_count_update_compute.map(s=>(s._1+" "+s._2+" "+s._3.mkString(",")))
//      .mkString("\n"))
    /**
      * 3 Query Redis
      *   1) 形成 Query Redis 所需的所有键
      *   2) Query
      */
    val t0_3=System.nanoTime()
    val Query:Array[Long]=newedges_count_update_compute.flatMap(s=>{
      val res=ArrayBuffer[Long]()
      res.append(s._2._1,s._2._2)
      res.appendAll(s._3(0).map(_._1))
      res.appendAll(s._3(1).map(_._1))
      res
    }).distinct
    println("Query: "+Query.length)
    //    +"-"+Query.mkString(" "))
    val Query_Answer:Map[Long,java.util.Map[Integer,java.lang.Long]]=redis_OP.Query_PB_Array(Query)
    val t1_3=System.nanoTime()
    println(" 3: Query Redis uses \t"+(t1_3-t0_3)/1e9)
//    println("Qurey_Answer: \n"+Query_Answer.mkString("\n"))
    /**
      * 4 根据 Query_Answer 进行计算 和 count更新
      *   注: 若不存在原边则进行新边计算，所有的新边都要进行count更新
      */
    /**
      * 4.2 计算新的边
      */
    val add_edges_buffer:ArrayBuffer[(Int,Int,Int)]=new ArrayBuffer(distinct_newedges_count*13)

    /**
      * 4.2.1 计算新边与新边产生的
      */
    val t0_4_2_1=System.nanoTime()
    add_edges_count.flatMap(es=>Array((es._1._1,(EncodeLabel_pos(es._1._3,1),es._1._2)), (es._1._2,(EncodeLabel_pos
    (es._1._3,0),es._1._1)))).groupBy(_._1).foreach(mid_edges=>{
      val lable_pos_nodes:Map[Int,Array[Int]]=mid_edges._2.map(s=>s._2).groupBy(_._1).map(s=>(s._1,s._2.map(e=>e._2)))
      for(lpn<-lable_pos_nodes){
        if(DecodeLabel_pos(lpn._1)._2==0){//为防止重复，只以f为标准计算
        val other_b_target_label_arrays=grammar_match.getOrElse(lpn._1,null)
          if(other_b_target_label_arrays!=null){
            val srcs=lpn._2
            for((other_b,targetlabel)<-other_b_target_label_arrays){
              val dsts=lable_pos_nodes.getOrElse(other_b,Array[Int]())
              for(dst<-dsts){
                for(src<-srcs){
                  add_edges_buffer.append((src,dst,targetlabel))
                }
              }
            }
          }
        }
      }
    })

    val t1_4_2_1=System.nanoTime()
    println(" 4.2.1: Compute new edges product new-new uses  \t"+(t1_4_2_1-t0_4_2_1)/1e9)
    println(" 4.2.1: Compute new edges product new-new count \t"+add_edges_buffer.size)
//    println(" new_new : "+add_edges_buffer.mkString("\n"))
    val num_new_new=add_edges_buffer.length
    /**
      * 4.2.2 计算新边与旧边产生
      */
      println("new_old:")

    val t0_4_2_2=System.nanoTime()
    newedges_count_update_compute.foreach(ecuc=>{
      val (src,dst,label)=ecuc._1._1
      val src_label_b=ecuc._2._1
      val dst_label_f=ecuc._2._2
      val filter_map=Query_Answer.getOrElse(src_label_b,null)
//      println(ecuc._1._1+"filter_map: "+filter_map)
//      if(filter_map!=null) println(s"filter_map.get($dst)= "+filter_map.getOrDefault(dst,null))
      if(filter_map==null||filter_map.getOrDefault(dst,null)==null){//说明此边原先不存在，可能产生新的边
        /**
          * for directadd
          */
        val add_label:Int=directadd.getOrElse(label,-1)
        if(add_label!= -1) add_edges_buffer.append((src,dst,add_label))

        /**
          * for grammar
          */
        val label_other_target=ecuc._3
//        println("label_other_target: "+label_other_target)
        /**
          * for src_x_f
          */
        for((src_x_f,label_A)<-label_other_target(0)){
          val uid_counts:java.util.Map[Integer,java.lang.Long]=Query_Answer.getOrElse(src_x_f,null)
          if(uid_counts!=null){
            val ks=ProtocolBuffer_OP.getmapKeys(uid_counts)
            for(k<-ks) add_edges_buffer.append((k, dst, label_A))
          }
        }
        /**
          * for dst_y_b
          */
        for((dst_y_b,label_B)<-label_other_target(1)){
          val uid_counts:java.util.Map[Integer,java.lang.Long]=Query_Answer.getOrElse(dst_y_b,null)
          if(uid_counts!=null){
            val ks=ProtocolBuffer_OP.getmapKeys(uid_counts)
            for(k<-ks) add_edges_buffer.append((src, k, label_B))
          }
        }

      }
    })
    val t1_4_2_2=System.nanoTime()
    println(" 4.2.2: Compute new edges product old-new uses  \t "+(t1_4_2_2-t0_4_2_2)/1e9)
    println(" 4.2.2: Compute new edges product old-new count \t "+(add_edges_buffer.length-num_new_new))
//    println("add_edges_buffer now: "+add_edges_buffer.mkString("\n"))
    val t0_5=System.nanoTime()
    val res=add_edges_buffer.toArray
    val t1_5=System.nanoTime()
    println(" 5: buffer to array uses \t"+(t1_5-t0_5)/1e9)
//    println(res.mkString("\n"))

    /**
      * 4.1 更新 Count 此处将转换为Byte数组的操作直接执行
      * 合并对同一个vid_label_pos的更新，此处不能直接应用map，不同的边可能会对同一个点进行更新
      */
    val t0_4_1=System.nanoTime()
    val key_toUpdateCounts=newedges_count_update_compute.flatMap(ele=>Array((ele._2._1,ele._1),(ele._2._2,ele._1)))
      .groupBy(_._1).toArray
    val key_num=key_toUpdateCounts.size
    val keys:Array[Array[Byte]]=new Array[Array[Byte]](key_num)
    val values:Array[Array[Byte]]=new Array[Array[Byte]](key_num)
    println("key_toUpdateCounts: "+key_toUpdateCounts.length)
    //      +"-"+key_toUpdateCounts.mkString(","))
    for(i<-0 to key_num-1) {
      val key_updateedges = key_toUpdateCounts(i)
      val src_dst_label = key_updateedges._2
      val key = key_updateedges._1
      val vid = DecodeVid_labelpos(key)._1
      keys(i) = Long2ByteArray(key)
      val targetMap: java.util.Map[Integer, java.lang.Long] = {
        val temp=Query_Answer.getOrElse(key, null)
        if(temp==null) new java.util.HashMap[Integer, java.lang.Long]()
        else temp
      }
      var (src,dst,label,count) = (0,0,0,0)
      for (ele <- src_dst_label) {
        count = ele._2._2
        src=ele._2._1._1
        dst=ele._2._1._2
        label=ele._2._1._3
        val encoded_counts = EncodeCount(count, step)
        if(vid==dst) ProtocolBuffer_OP.UpdateCounts(targetMap,src,encoded_counts)
        else ProtocolBuffer_OP.UpdateCounts(targetMap,dst,encoded_counts)
      }
      values(i) = ProtocolBuffer_OP.Serialzed_Map_UidCounts(targetMap)
    }
    redis_OP.Update_PB_ByteArray(keys,values)
    val t1_4_1=System.nanoTime()
    println(" 4.1:Update Counts uses \t"+(t1_4_1-t0_4_1)/1e9)
    res
  }

  def ComputeDistribution(add_edges:RDD[(Int,Int,Int)],grammar_match:Map[Int,Array[(Int,Int)]],
                          loop:List[Int],
                          directadd:Map[Int,Int],step:Int,
                          redis_OP:Redis_OP):RDD[(Int, Int,Int)]={
    /**
      * 1 统计数据
      */
    var add_edges_count:RDD[((Int,Int,Int),Int)]=add_edges.map(e=>(e,1)).reduceByKey(add_edges.partitioner.getOrElse
    (new HashPartitioner(Param_pt.clusterpar)),(x,y)=>x+y)
    println("add_edges_count: "+add_edges_count.count())//+" \n"+add_edges_count.collect().mkString("\n")
    //val distinct_newedges_count=add_edges_count.length
    /**
      * 2 计算所需查询的所有key，包括
      *   1) Update_Count所需的key : src_label_b and dst_label_f ，其中任选其一可作为filter
      *   2) Compute new edges 所需的key : [src_x_f] and [dst_y_b]
      *
      *   数据格式: (
      *     ((src,dst,label),count)
      *     (src_label_b,dst_label_f),
      *     Array[Arraybuffer[vid_label_pos,label_target]] : [(src_x_f,A)] and [(dst_y_b,B)]
      *     )
      */
    //
    val newedges_count_update_compute:RDD [(((Int,Int,Int),Int),(Long,Long),Array[ArrayBuffer[(Long,Int)]])]=
    add_edges_count.map(ec=>{
      val (src,dst,label)=ec._1
      val update_src=EncodeVid_label_pos(src,label,1)
      val update_dst=EncodeVid_label_pos(dst,label,0)
      val compute=new Array[ArrayBuffer[(Long,Int)]](2)
      for(i<-0 to 1)
        compute(i)=new ArrayBuffer[(Long, Int)]()
      grammar_match.getOrElse(EncodeLabel_pos(label,1),Array[(Int,Int)]())
        .foreach(s=> {
          val (x_f, label_A) = s
          compute(0).append((EncodeVid_labelpos(src, x_f), label_A))
        })
      grammar_match.getOrElse(EncodeLabel_pos(label,0),Array[(Int,Int)]()).foreach(s=>{
        val (y_b,label_B)=s
        compute(1).append((EncodeVid_labelpos(dst,y_b),label_B))
      })
      (ec,(update_src,update_dst),compute)
    }).persist(StorageLevel.MEMORY_ONLY_SER)
    println("newedges_count_update_compute.count: "+newedges_count_update_compute.count())
//    println("newedges_count_update_compute: \n"+newedges_count_update_compute.collect().map(s=>(s._1+" "+s._2+" "+s._3.mkString(","))).mkString("\n"))
    /**
      * 3 Query Redis
      *   1) 形成 Query Redis 所需的所有键
      *   2) 分配到RDD的每条记录中
      */
    val newedges_count_update_compute_Answer:RDD[(((Int,Int,Int),Int),(Long,Long),Array[ArrayBuffer[(Long,Int)]],(java.util.Map[Integer,java.lang
    .Long],java.util.Map[Integer,java.lang.Long]),Array[Array[java.util.Map[Integer,java.lang.Long]]])
      ]=newedges_count_update_compute.mapPartitions(p_ecuc=>redis_OP.QueryAndUpdate_PB_RDD_Partition(p_ecuc)).persist
    (StorageLevel.MEMORY_ONLY_SER)
    println("newedges_count_update_compute_Answer.count: "+newedges_count_update_compute_Answer.count())
//    println("newedges_count_update_compute_Answer: \n"+newedges_count_update_compute_Answer.collect().map(s=>(s._1+" " +
//      ""+s._2+" "+s._3.mkString(",")+" "+s._4+" "+s._5(0).mkString(";")+" and "+s._5(1).mkString(";"))).mkString("\n"))
    //    val temp=newedges_count_update_compute_Answer.collect()

    /**
      * 4 根据 Query_Answer 进行计算 和 count更新
      *   注: 若不存在原边则进行新边计算，所有的新边都要进行count更新
      */

    /**
      * 4.2 计算新的边
      */
    /**
      * 4.2.1 计算新边与新边产生的
      */
    val newedges_nn:RDD[(Int,Int,Int)]=add_edges_count
      .flatMap(es=>Array((es._1._1,(EncodeLabel_pos(es._1._3,1),es._1._2)), (es._1._2, (EncodeLabel_pos(es._1._3,0),es._1._1))))
      .groupByKey()
      .flatMap(mid_edges=>{
        val res:ArrayBuffer[(Int,Int,Int)]=ArrayBuffer()
        val lable_pos_nodes:Map[Int,Array[Int]]=mid_edges._2.groupBy(_._1).map(s=>(s._1,s._2.map(e=>e._2).toArray))
        for(lpn<-lable_pos_nodes){
          if(DecodeLabel_pos(lpn._1)._2==0){//为防止重复，只以f为标准计算
          val other_b_target_label_arrays=grammar_match.getOrElse(lpn._1,null)
            if(other_b_target_label_arrays!=null){
              val srcs=lpn._2
              for((other_b,targetlabel)<-other_b_target_label_arrays){
                val dsts=lable_pos_nodes.getOrElse(other_b,Array[Int]())
                for(dst<-dsts){
                  for(src<-srcs){
                    res.append((src,dst,targetlabel))
                  }
                }
              }
            }
          }
        }
        res
      }).persist(StorageLevel.MEMORY_ONLY_SER)
    println("newedges_nn.count: \t"+newedges_nn.count())
//    println("newedges_nn: \t"+newedges_nn.collect().mkString("\n"))

    println("newedges_count_update_compute_Answer.count : "+newedges_count_update_compute_Answer.count())
    val newedges_on:RDD[(Int,Int,Int)]=
      newedges_count_update_compute_Answer.mapPartitions(p=>{
        val arrays=new ArrayBuffer[(Int,Int,Int)](1024)
        p.foreach(s=>{
//          println(s._1)
          val src_label_b=s._2._1
          val src_label_b_map=s._4._1
          val (src,dst,label)=s._1._1
//          println("src_label_b_map: "+src_label_b_map)
          if(src_label_b_map==null||src_label_b_map.getOrDefault(dst,null)==null){
            val res:ArrayBuffer[(Int,Int,Int)]=new ArrayBuffer[(Int, Int, Int)]()
            /**
              * for directadd
              */
            val label_add=directadd.getOrElse(label,-1)
            if(label_add!= -1) res.append((src,dst,label_add))
            /**
              * for grammar
              */
            val label_other_target=s._3
            /**
              * for src_x_f
              */
            var (src_x_f,label_A)=(0l,0)
            for(i<-0 to label_other_target(0).length-1){
              val uid_counts:java.util.Map[Integer,java.lang.Long]=s._5(0)(i)
              if(uid_counts!=null){
                src_x_f=label_other_target(0)(i)._1
                label_A=label_other_target(0)(i)._2
                val ks=ProtocolBuffer_OP.getmapKeys(uid_counts)
                for(k<-ks) res.append((k, dst, label_A))
              }
            }
            /**
              * for dst_y_b
              */
            var (dst_y_b,label_B)=(0l,0)
            for(i<-0 to label_other_target(1).length-1){
              val uid_counts:java.util.Map[Integer,java.lang.Long]=s._5(1)(i)
              if(uid_counts!=null){
                dst_y_b=label_other_target(1)(i)._1
                label_B=label_other_target(1)(i)._2
                val ks=ProtocolBuffer_OP.getmapKeys(uid_counts)
                for(k<-ks) res.append((src, k, label_B))
              }
            }
            println("res: "+res.length)
            arrays.appendAll(res)
          }
        })
        println("arrays: "+arrays.length)
        arrays.toIterator
      }).persist(StorageLevel.MEMORY_ONLY_SER)
    println("newedges_on.count: \t"+newedges_on.count())
//    println("newedges_on: \t"+newedges_on.collect().mkString("\n"))

    /**
      * 4.1 合并对同一个vid_label_pos的更新，此处不能直接应用map，不同的边可能会对同一个点进行更新
      */
    val UpdateCounts:RDD[(Long,java.util.Map[Integer,java.lang.Long])]=
    newedges_count_update_compute_Answer.flatMap(s=>Array((s._2._1,(s._1, s._4._1)),(s._2._2,(s._1,s._4._2))))
      .groupByKey()
      .map(s=>{
        val array=s._2.toArray
        (s._1,array(0)._2,array.map(_._1))
      })
      .mapPartitions(ss=>ss.map(s=>{
        val vid=DecodeVid_labelpos(s._1)._1
        val targetMap={
          if(s._2==null) new util.HashMap[Integer,java.lang.Long]()
          else s._2
        }
        var (count,src,dst,label)=(0,0,0,0)
        var encodedcounts=0L
        for(src_dst_label_count<-s._3){
          count=src_dst_label_count._2
          encodedcounts=EncodeCount(count,step)
          src=src_dst_label_count._1._1
          dst=src_dst_label_count._1._2
          label=src_dst_label_count._1._3
          if(src==vid) ProtocolBuffer_OP.UpdateCounts(targetMap,dst,encodedcounts)
          else ProtocolBuffer_OP.UpdateCounts(targetMap,src,encodedcounts)
        }
        (s._1,targetMap)
      }))
    println("UpdateCounts.count: "+UpdateCounts.count())
//    println("UpdateCounts: "+UpdateCounts.collect().mkString("\n"))
    redis_OP.Update_PB(UpdateCounts)
    //    println("newedges_count_update_compute_Answer.count : "+newedges_count_update_compute_Answer.count())
    val res=newedges_nn.union(newedges_on).setName(s"add_edges_rdd $step").persist(StorageLevel.MEMORY_ONLY_SER)
//    println(res.collect().mkString("\n"))
    res
  }

}
