package ONLINE

import java.util
import java.util.Scanner

import ONLINE.ProtocolBuffer.ProtocolBuffer_OP
import ONLINE.utils_ONLINE.{BIgSpa_OP, Param_pt, Redis_OP}
import cn.edu.nju.pasalab.db.ShardedRedisClusterClient
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

import collection.JavaConversions
import scala.collection.mutable.ArrayBuffer
import scala.io.Source
/**
  * Created by cycy on 2019/3/5.
  */
object Query_Filter_Compute_Update {
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
    val (pos,label)=DecodeLabel_pos(label_pos.toInt)
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
    //    println("------------Spark and HBase settings--------------------------------")
    //    println("spark.driver.memory:          \t" + conf.get("spark.driver.memory"))
    //    println("spark.executor.memory:        \t" + conf.get("spark.executor.memory"))
    //    println("spark.executor.instances:     \t" + conf.get("spark.executor.instances"))
    //    println("spark.executor.cores:         \t" + conf.get("spark.executor.cores"))
    //    println("default partition num:        \t" + Param_pt.defaultpar)
    //    println("cluster partition num:        \t" + Param_pt.clusterpar)
    //    println("updateRedis_interval:          \t" + Param_pt.updateRedis_interval)
    //    println("queryRedis_interval:          \t" + Param_pt.queryRedis_interval)
    //    println("--------------------------------------------------------------------")
    //    println

    /**
      * Grammar相关设置
      */
    val grammar_origin = sc.textFile(Param_pt.input_grammar).filter(s => !s.trim.equals("")).map(s => s.split("\\s+").map(_
      .trim))
      .collect().toList
    val (symbol_Map, symbol_num, symbol_num_bitsize, loop, directadd, grammar) = BIgSpa_OP.processGrammar(grammar_origin,
      Param_pt.input_grammar)
    //    println("------------Grammar INFO--------------------------------------------")
    //    println("input grammar:      \t" + Param_pt.input_grammar.split("/").last)
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
    //    grammar.foreach(s => println("                    \t" + s(0) + "\t+\t" + s(1) + "\t->\t" + s(2)))
    //    println("---------------------------------------------------------------------")
    //    println

    val grammar_match:Map[Int,Array[(Int,Int)]]=parseGrammar(grammar)

    /**
      * 设置Redis
      */
    ShardedRedisClusterClient.getProcessLevelClient.clearDB()
    val redis_OP = new Redis_OP(Param_pt.updateRedis_interval)

    /**
      * RDD读入原边集
      */
    val original_edges:RDD[(Long,java.util.Map[Integer,java.lang.Long])]=sc.textFile(Param_pt.input_graph,Param_pt
      .clusterpar).map(line=>{
      val parts=line.split(":")
      val part2_split=parts(1).split("\\s+")
      val len=part2_split.length
      if(len%2!=0) println("error")
      val javaMap=new util.HashMap[Integer,java.lang.Long]()
      Array.range(0,len).map(index=>javaMap.put(part2_split(index).toInt,part2_split(index+1).toLong))
      (parts(0).toLong,javaMap)
    })

    redis_OP.Update_PB(original_edges)

    /**
      * Streaming
      */
    val changemode_threshold:Int=1000
    val scan=new Scanner(System.in)

    while(true){
      val source:Array[String]=Source.fromFile("").getLines().toArray
      //source 的数量保证小批量
      var add_edges: Array[(Int, Int, Int)] =source.map(line=>{
        val s=line.split(",")
        (s(0).toInt,s(1).toInt,s(2).toInt)
      })
      var add_edges_rdd:RDD[(Int,Int,Int)]=null

      var step=0//为0表示直接添加，>0表示三角添加
      var add_edges_len=add_edges.length
      if(add_edges_len<changemode_threshold){
        if(add_edges==null) {
          add_edges=add_edges_rdd.collect()
          add_edges_rdd.unpersist()
          add_edges_rdd=null
        }
        add_edges=ComputeSingleMachine(add_edges,grammar_match,step,redis_OP)
        add_edges_len=add_edges.length
      }
      else{
        if(add_edges_rdd==null){
          add_edges_rdd=sc.parallelize(add_edges,Param_pt.clusterpar)
          add_edges=null
        }
        add_edges_rdd=ComputeDistribution(add_edges_rdd,grammar_match,step,redis_OP)
        add_edges_len=add_edges_rdd.count().toInt
      }
      step+=1
    }

  }

  def ComputeSingleMachine(add_edges:Array[(Int,Int,Int)],grammar_match:Map[Int,Array[(Int,Int)]],step:Int,
                           redis_OP:Redis_OP):Array[(Int,Int,Int)]={
    /**
      * 1 统计数据
      */
    var add_edges_count:Array[((Int,Int,Int),Int)]=add_edges.map(e=>(e,1)).groupBy(_._1).map(eg=>(eg._1,eg._2.length)).toArray

    val distinct_newedges_count=add_edges_count.length
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
    val newedges_count_update_compute:Array[(((Int,Int,Int),Int),(Long,Long),Array[ArrayBuffer[(Long,Int)]])]=
    add_edges_count.map(ec=>{
      val (src,dst,label)=ec._1
      val update_src=EncodeVid_label_pos(src,label,1)
      val update_dst=EncodeVid_label_pos(dst,label,0)
      val compute=new Array[ArrayBuffer[(Long,Int)]](2)
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

    /**
      * 3 Query Redis
      *   1) 形成 Query Redis 所需的所有键
      *   2) Query
      */
    val Query:Array[Long]=newedges_count_update_compute.flatMap(s=>{
      val res=ArrayBuffer[Long]()
      res.append(s._2._1,s._2._2)
      res.appendAll(s._3(0).map(_._1))
      res.appendAll(s._3(1).map(_._1))
      res
    })
    val Query_Answer:Map[Long,java.util.Map[Integer,java.lang.Long]]=redis_OP.Query_PB_Array(Query)

    /**
      * 4 根据 Query_Answer 进行计算 和 count更新
      *   注: 若不存在原边则进行新边计算，所有的新边都要进行count更新
      */

    /**
      * 4.1 更新 Count 此处将转换为Byte数组的操作直接执行
      * 合并对同一个vid_label_pos的更新，此处不能直接应用map，不同的边可能会对同一个点进行更新
      */
    val key_num=Query_Answer.size
    val keys:Array[Array[Byte]]=new Array[Array[Byte]](key_num)
    val values:Array[Array[Byte]]=new Array[Array[Byte]](key_num)
    val key_toUpdateCounts=newedges_count_update_compute.flatMap(ele=>Array((ele._2._1,ele._1),(ele._2._2,ele._1)))
      .groupBy(_._1).toArray
    for(i<-0 to key_num-1) {
      val key_updateedges = key_toUpdateCounts(i)
      val src_dst_label = key_updateedges._2
      val key = key_updateedges._1
      val vid = DecodeVid_labelpos(key)._1
      keys(i) = Long2ByteArray(key)
      val targetMap: java.util.Map[Integer, java.lang.Long] = Query_Answer.getOrElse(key, new util.HashMap[Integer,java
      .lang.Long]())
      var (src,dst,label,count) = (0,0,0,0)
      for (ele <- src_dst_label) {
        count = ele._2._2
        (src, dst, label) = ele._2._1
        val encoded_counts = EncodeCount(count, step)
        if(vid==dst) ProtocolBuffer_OP.UpdateCounts(targetMap,src,encoded_counts)
        else ProtocolBuffer_OP.UpdateCounts(targetMap,dst,encoded_counts)
      }
      values(i) = ProtocolBuffer_OP.Serialzed_Map_UidCounts(targetMap)
    }
      redis_OP.Update_PB_ByteArray(keys,values)

      /**
        * 4.2 计算新的边
        */
      val add_edges_buffer:ArrayBuffer[(Int,Int,Int)]=new ArrayBuffer(distinct_newedges_count*13)
      newedges_count_update_compute.foreach(ecuc=>{
        val (src,dst,label)=ecuc._1._1
        val src_label_b=ecuc._2._1
        val dst_label_f=ecuc._2._2
        val filter_map=Query_Answer.getOrElse(src_label_b,null)
        if(filter_map==null||filter_map.getOrDefault(dst,null)==null){//说明此边原先不存在，可能产生新的边
        val label_other_target=ecuc._3

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

    add_edges_buffer.toArray
    }

    def ComputeDistribution(add_edges:RDD[(Int,Int,Int)],grammar_match:Map[Int,Array[(Int,Int)]],step:Int,
                            redis_OP:Redis_OP):RDD[(Int, Int,Int)]={
      /**
        * 1 统计数据
        */
      var add_edges_count:RDD[((Int,Int,Int),Int)]=add_edges.map(e=>(e,1)).reduceByKey(add_edges.partitioner.getOrElse
      (new HashPartitioner(Param_pt.clusterpar)),(x,y)=>x+y)
      //val distinct_newedges_count=add_edges_count.length
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
      val newedges_count_update_compute:RDD [(((Int,Int,Int),Int),(Long,Long),Array[ArrayBuffer[(Long,Int)]])]=
      add_edges_count.map(ec=>{
        val (src,dst,label)=ec._1
        val update_src=EncodeVid_label_pos(src,label,1)
        val update_dst=EncodeVid_label_pos(dst,label,0)
        val compute=new Array[ArrayBuffer[(Long,Int)]](2)
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

      /**
        * 3 Query Redis
        *   1) 形成 Query Redis 所需的所有键
        *   2) 分配到RDD的每条记录中
        */
      val newedges_count_update_compute_Answer:RDD[(((Int,Int,Int),Int),(Long,Long),Array[ArrayBuffer[(Long,Int)]],(java.util.Map[Integer,java.lang
      .Long],java.util.Map[Integer,java.lang.Long]),Array[Array[java.util.Map[Integer,java.lang.Long]]])
        ]=newedges_count_update_compute.mapPartitions(p_ecuc=>redis_OP.QueryAndUpdate_PB_RDD_Partition(p_ecuc))


      /**
        * 4 根据 Query_Answer 进行计算 和 count更新
        *   注: 若不存在原边则进行新边计算，所有的新边都要进行count更新
        */

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
            (src,dst,label)=src_dst_label_count._1
            if(src==vid) ProtocolBuffer_OP.UpdateCounts(targetMap,dst,encodedcounts)
            else ProtocolBuffer_OP.UpdateCounts(targetMap,src,encodedcounts)
          }
          (s._1,targetMap)
      }))

      redis_OP.Update_PB(UpdateCounts)
      /**
        * 4.2 计算新的边
        */
      newedges_count_update_compute_Answer.mapPartitions(p=>{
        val arrays=new ArrayBuffer[(Int,Int,Int)](1024)
        p.map(s=>{
          val src_label_b=s._2._1
          val src_label_b_map=s._4._1
          val (src,dst,label)=s._1._1
          if(src_label_b_map==null||src_label_b_map.getOrDefault(dst,null)==null){
            val res:ArrayBuffer[(Int,Int,Int)]=new ArrayBuffer[(Int, Int, Int)]()
            val label_other_target=s._3
            /**
              * for src_x_f
              */
            var (src_x_f,label_A)=(0,0)
            for(i<-0 to label_other_target(0).length-1){
              val uid_counts:java.util.Map[Integer,java.lang.Long]=s._5(0)(i)
              if(uid_counts!=null){
                (src_x_f,label_A)=label_other_target(0)(i)
                val ks=ProtocolBuffer_OP.getmapKeys(uid_counts)
                for(k<-ks) res.append((k, dst, label_A))
              }
            }
            /**
              * for dst_y_b
              */
            var (dst_y_b,label_B)=(0,0)
            for(i<-0 to label_other_target(1).length-1){
              val uid_counts:java.util.Map[Integer,java.lang.Long]=s._5(1)(i)
              if(uid_counts!=null){
                (dst_y_b,label_B)=label_other_target(1)(i)
                val ks=ProtocolBuffer_OP.getmapKeys(uid_counts)
                for(k<-ks) res.append((src, k, label_B))
              }
            }
            arrays.appendAll(res)
          }
        })
        arrays.toIterator
      })
    }

  }
