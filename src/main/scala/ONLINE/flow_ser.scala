/**
  * Created by cycy on 2019/4/10.
  */
package ONLINE


import java.io.{File, PrintWriter}
import java.util
import java.lang
import java.text.SimpleDateFormat
import java.util.Scanner
import javax.management

import ONLINE.ProtocolBuffer.ProtocolBuffer_OP
import ONLINE.ProtocolBuffer.ProtocolBuffer_OP._
import ONLINE.utils_ONLINE.{BIgSpa_OP, Param_pt, Redis_OP}
import cn.edu.nju.pasalab.db.{BasicKVDatabaseClient, ShardedRedisClusterClient, Utils}
import it.unimi.dsi.fastutil.ints.IntOpenHashSet
import it.unimi.dsi.fastutil.longs.LongOpenHashSet
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConversions.mapAsScalaMap
import scala.io.Source
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
object flow_ser {
  var totaladd=0l
  var flowadd=0l
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

  def printTime(str:String,t0:Double,t1:Double): Unit ={
    println( str+((t1 - t0) / 1000000000.0).formatted("%" + ".3f"))
  }
  /**
    * Main
    * @param args
    */
  def main(args: Array[String]): Unit = {
    var t0=System.nanoTime()
    var t1=System.nanoTime()
    val scan=new Scanner(System.in)
    val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
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

    val f = Source.fromFile(Param_pt.add).getLines()
    /**
      * Streaming
      */

    println("FLOW START AT "+df.format(System.currentTimeMillis()))
    var addturn=0
    t0=System.nanoTime()
    for(add<-f){
      println("\n************During add turn " + addturn + s"\t$add\t************")
      flowadd=0
      val t0_delta=System.nanoTime()
      val worklist=new ArrayBuffer[((Int,Int,Int),Int)]()
      worklist.appendAll(Source.fromFile(add).getLines().toArray.filter(! _.trim.equals("")).map(line => {
        val s = line.split("\\s+")
        val (src, dst, label) = (s(0).toInt, s(1).toInt, symbol_Map.getOrElse(s(2), -1))
        if (label == -1) println("there is some label wrong in input")
        ((src,dst,label),1)
      }))
      var step=0
      while(worklist.length>0)
      {
        val ((src,dst,label),count)=worklist.remove(0)
        val res=new ArrayBuffer[(Int,Int,Int)]()
//        println(s"e: $src,$dst,$label")
        //filter
        val updatesrckey=EncodeVid_label_pos(src,label,1)
        val updatesrcMap={
          val temp=redis_OP.QueryforOneRecord(updatesrckey)
          if(temp==null) new util.HashMap[Integer,lang.Long]()
          else temp
        }

        if(updatesrcMap.containsKey(dst)==false){
          totaladd+=1
//          println(s"new edge ($src,$dst,$label)")
          //compute
          //label在前
          if (grammar_match.getOrElse(EncodeLabel_pos(label, 0), null) != null) {
            for ((otherlabelpos, target) <- grammar_match.getOrElse(EncodeLabel_pos(label, 0), null)) {
              val otherlabel = DecodeLabel_pos(otherlabelpos)._1
              val key = EncodeVid_label_pos(dst, otherlabel, 1)
              val map = redis_OP.QueryforOneRecord(key)
              if(map!=null){
                val others=getmapKeys(map)
                res.appendAll(others.map(s=>(src,s.toInt,target)))
                println(others.length)
              }
            }
          }
          //label在后
          if (grammar_match.getOrElse(EncodeLabel_pos(label, 1), null) != null) {
            for ((otherlabelpos, target) <- grammar_match.getOrElse(EncodeLabel_pos(label, 1), null)) {
              val otherlabel = DecodeLabel_pos(otherlabelpos)._1
              val key = EncodeVid_label_pos(src, otherlabel, 0)
              val map = redis_OP.QueryforOneRecord(key)
              if(map!=null){
                val others=getmapKeys(map)
                res.appendAll(others.map(s=>(s.toInt,dst,target)))
                println(others.length)
              }
            }
          }

          //directadd
          if(directadd.getOrElse(label,-1)!= -1){
            val dir_label=directadd.getOrElse(label,-1)
            res.append((src,dst,dir_label))
            println("directadd")
          }
        }
        //update
        ProtocolBuffer_OP.UpdateCounts(updatesrcMap,dst,EncodeCount(count,step))
        redis_OP.UpdateforOneRecord(updatesrckey,updatesrcMap)

        val updatedstkey=EncodeVid_label_pos(dst,label,0)
        val updatedstMap={
          val temp=redis_OP.QueryforOneRecord(updatedstkey)
          if(temp==null) new util.HashMap[Integer,lang.Long]()
          else temp
        }
        ProtocolBuffer_OP.UpdateCounts(updatedstMap,src,EncodeCount(count,step))
        redis_OP.UpdateforOneRecord(updatedstkey,updatedstMap)
        worklist.appendAll(res.map((_,1)).groupBy(_._1).map(t=>(t._1,t._2.size)))
      step+=1
      }
      val t1_delta=System.nanoTime()

      println(s"update takes time\t"+(t1_delta-t0_delta)/1e9)
      println(s"addturn\t$addturn\taddfile\t$add\tflowadd\t$flowadd\ttotaladd\t$totaladd")
      addturn+=1
    }

    println("FLOW END AT "+df.format(System.currentTimeMillis()))
    println(s"FLOW ADD　TOTAL EDGES $totaladd")
    println("average time: "+(System.nanoTime()-t0)/1e11)

  }

  def ComputeFile(add_edges0: Array[(Int, Int, Int)],grammar_match:Map[Int,Array[(Int,Int)]],
                  directadd:Map[Int,Int],redis_OP:Redis_OP,sc:SparkContext):Int={
    flowadd=0
    //source 的数量保证小批量
    val t0_delta=System.nanoTime()

    var add_edges=add_edges0
    var add_edges_rdd: RDD[(Int, Int, Int)] = null

    var step = 0 //为0表示直接添加，>0表示三角添加
    var add_edges_len = add_edges.length
    while (add_edges_len > 0 && (System.nanoTime()-t0_delta)/1e9 <60 ) {
      val t0_1=System.nanoTime()
      if (add_edges_len < Param_pt.changemode_interval) {
        //        print("SingleMachine : ")
        if (add_edges == null) {
          add_edges = add_edges_rdd.collect()
          add_edges_rdd.unpersist()
          add_edges_rdd = null
        }
        add_edges = ComputeSingleMachine_ADD(add_edges, grammar_match,directadd, step, redis_OP)
        add_edges_len = add_edges.length

      }
      else {
        //        print("Distributed : ")
        if (add_edges_rdd == null) {
          add_edges_rdd = sc.parallelize(add_edges, Param_pt.clusterpar)
          add_edges = null
        }
        add_edges_rdd = ComputeDistribution(add_edges_rdd, grammar_match, directadd, step, redis_OP)
        add_edges_len = add_edges_rdd.count().toInt
        //          println("add_edges count: "+add_edges_rdd.count())
      }
      val t1_1=System.nanoTime()
      //      println(s"step $step uses " + (t1_1-t0_1)/1e9 )
      step += 1
      //        val pause=scan.nextLine()
    }
    step
  }

  def ComputeDistribution(add_edges:RDD[(Int,Int,Int)],grammar_match:Map[Int,Array[(Int,Int)]],
                          directadd:Map[Int,Int],step:Int,
                          redis_OP:Redis_OP):RDD[(Int, Int,Int)]={
    /**
      * 1 统计数据
      */
    var add_edges_count_0:RDD[((Int,Int,Int),Int)]=add_edges.map(e=>(e,1)).reduceByKey(add_edges.partitioner.getOrElse
    (new HashPartitioner(Param_pt.clusterpar)),(x,y)=>x+y)
    //    println("add_edges_count_0: "+add_edges_count_0.count())//+" \n"+add_edges_count.collect().mkString("\n")
    //val distinct_newedges_count=add_edges_count.length
    /**
      * 2 计算Update和filter所需的key，包括
      *   1) Update_Count所需的key : src_label_b and dst_label_f ，其中任选其一可作为filter
      *
      *   数据格式: (
      *     (src,dst,label),
      *     (src_label_b,dst_label_f),
      *     Array[Arraybuffer[vid_label_pos,label_target]] : [(src_x_f,A)] and [(dst_y_b,B)]
      *     )
      */

    val key_Update_Filter:RDD[(Long,Array[(Int,Long)])] = {
      add_edges_count_0.flatMap(ec=>{
        val ((src,dst,label),count)=ec
        val encodedcounts=EncodeCount(count,step)
        Array((EncodeVid_label_pos(src,label,1),(dst,encodedcounts)),(EncodeVid_label_pos(dst,label,0),(src,encodedcounts)))
      }).groupByKey().map(s=>(s._1,s._2.toArray[(Int,Long)]))
    }
    val t1_2_1_1=System.nanoTime()
    //    println(" \t2.1.1 form Key_Update_filter "+key_Update_Filter.size+" uses "+(t1_2_1_1-t0_2)/1e9)
    val key_answer_counts_Update_Filter:RDD[(Long,(Array[(Int,Long)],util.Map[Integer,lang.Long]))]=
    redis_OP.Query_PBRDD_withCounts(key_Update_Filter)
    val t1_2_1_2=System.nanoTime()
    //    println(" \t2.1.2 form key_answer_Update_Filter uses "+(t1_2_1_2-t1_2_1_1)/1e9)

    val add_edges_count_purenew:RDD[(Int,Int,Int)]=add_edges_count_0.map(s=>(EncodeVid_label_pos(s._1._1,s._1._3,1),s._1))
      .leftOuterJoin(key_answer_counts_Update_Filter).filter(s=>{
      val map=s._2._2.head._2
      if(map==null||map.getOrDefault(s._2._1._2,null)==null) true
      else false
    }).map(s=>s._2._1)

    //    println("add pure new : "+add_edges_count_purenew.count())
    flowadd+=add_edges_count_purenew.count()

    val t1_2_2=System.nanoTime()
    //    println(" \t2.2 form clean new edges uses \t"+(t1_2_2-t1_2_1_2)/1e9)
    //    println("add_edges_count_pure_new.count: \t"+add_edges_count_purenew.count())
    //    println("newedges_count_update_compute: \n"+newedges_count_update_compute.map(s=>(s._1+" "+s._2+" "+s._3.mkString(",")))
    //      .mkString("\n"))

    /**
      * 3 Query Redis
      *   1) 形成 Compute 所需的所有键
      *   2) Query
      */
    val t0_3=System.nanoTime()

    // mid, Map[label_pos, Array[uid]]
    val mid_neighbour:RDD[(Int,Map[Int,Array[Int]])]=add_edges_count_purenew.flatMap(s=>Array(
      (s._1,(EncodeLabel_pos(s._3,1),s._2)),(s._2,(EncodeLabel_pos(s._3,0),s._1))))
      .groupByKey()
      .map(s=>(s._1,s._2.groupBy(_._1).map(x=>(x._1,x._2.toArray.map(_._2)))))
    //    val t1_3_1=System.nanoTime()
    //    println(" \t3.1 form mid_neighbours uses "+(t1_3_1-t0_3)/1e9)

    /**
      * new * new
      */
    val nn:RDD[(Int,Int,Int)]=mid_neighbour.flatMap(s=>{
      val res=new ArrayBuffer[(Int,Int,Int)]()
      for(label_f_uids<- s._2){
        if(label_f_uids._1%2==0){//f
          /**
            * new * new
            */
          val(label_f,uid_src)=label_f_uids
          val label_b_targetlabels=grammar_match.getOrElse(label_f,null)
          if(label_b_targetlabels!=null){//有可匹配的label_pos
            for((label_b,targetlabel)<-label_b_targetlabels){
              val uid_dst=s._2.getOrElse(label_b,null)
              if(uid_dst!=null){
                uid_src.foreach(src=>res.appendAll(uid_dst.map(dst=>(src,dst,targetlabel))))
              }
            }
          }

          /**
            * dierectadd : Mq::=M
            */
          val label=DecodeLabel_pos(label_f)._1
          val directadd_label=directadd.getOrElse(label,-1)
          if(directadd_label!= -1) {
            val mid = s._1
            res.appendAll(uid_src.map(src => (src,mid,directadd_label)))
          }
        }
      }
      res
    })

    val Compute_Key:RDD[Long]=add_edges_count_purenew.flatMap(s=>{
      val res=new ArrayBuffer[Long](16)
      val (src,dst,label)=s
      /**
        * src label f
        */
      val label_b=EncodeLabel_pos(label,1)
      val label_f_others=grammar_match.getOrElse(label_b,null)
      if(label_f_others!=null){
        res.appendAll(label_f_others.map(x=>EncodeVid_labelpos(src,x._1)))
      }
      /**
        * dst label b
        */
      val label_f=EncodeLabel_pos(label,0)
      val label_b_others=grammar_match.getOrElse(label_f,null)
      if(label_b_others!=null){
        res.appendAll(label_b_others.map(x=>EncodeVid_labelpos(dst,x._1)))
      }
      res
    }).distinct
    val Compute_Key_Answer:RDD[(Long,java.util.Map[Integer,java.lang.Long])]=redis_OP.Query_PBRDD_Long(Compute_Key)
    val t1_3=System.nanoTime()
    //    println(" \t3: Query Redis for Compute uses \t"+(t1_3-t0_3)/1e9)

    //mid label_pos -> uids
    val old_mid_neighbours:RDD[(Int,Map[Int,Array[Int]])]=Compute_Key_Answer.map(s=>{
      val (mid,label_pos)=DecodeVid_labelpos(s._1)
      val uids:Array[Int]={
        if(s._2!=null) ProtocolBuffer_OP.getmapKeys(s._2).map(_.toInt)
        else null
      }
      (mid,(label_pos,uids))
    }).groupByKey().map(s=>(s._1,s._2.toMap))

    /**
      * new * old
      */

    val no:RDD[(Int,Int,Int)]=mid_neighbour.leftOuterJoin(old_mid_neighbours).flatMap(s=>{
      val res=new ArrayBuffer[(Int,Int,Int)]()
      val (newedges,oldedges0)=s._2
      if(oldedges0.isEmpty==false){
        val oldedges=oldedges0.head
        for((new_labelpos,new_uids)<-newedges){
          val old_labelpos_targets=grammar_match.getOrElse(new_labelpos,null)
          if(old_labelpos_targets!=null){
            for((old_labelpos,targetlabel)<-old_labelpos_targets){
              val old_uids=oldedges.getOrElse(old_labelpos,null)
              if(old_uids!=null){
                if(new_labelpos%2==0){ //new_uids 在前
                  new_uids.foreach(src=>res.appendAll(old_uids.map(dst=>(src,dst,targetlabel))))
                }
                else{ // old_uids在前
                  old_uids.foreach(src=>res.appendAll(new_uids.map(dst=>(src,dst,targetlabel))))
                }
              }
            }
          }
        }
      }
      res
    })

    val res=(nn union no).persist(StorageLevel.MEMORY_ONLY).setName(s"Distributed add edges at step $step")
    if(step%Param_pt.checkpoint_interval==0) res.checkpoint()
    res.take(1)
    //    println("Distributed add new edges : "+res.count())

    /**
      * Update Redis
      */
    val Key_Maps:RDD[(Long,util.Map[Integer,lang.Long])]=
    key_answer_counts_Update_Filter.map(s=>{
      val map={
        if(s._2._2==null) new util.HashMap[Integer,lang.Long]()
        else s._2._2
      }
      s._2._1.foreach(x=>ProtocolBuffer_OP.UpdateCounts(map,x._1,x._2))
      (s._1,map)
    })
    redis_OP.Update_PB(Key_Maps)
    res
    //    val newedges_count_update_compute:RDD [(((Int,Int,Int),Int),(Long,Long),Array[ArrayBuffer[(Long,Int)]])]=
    //    add_edges_count.map(ec=>{
    //      val (src,dst,label)=ec._1
    //      val update_src=EncodeVid_label_pos(src,label,1)
    //      val update_dst=EncodeVid_label_pos(dst,label,0)
    //      val compute=new Array[ArrayBuffer[(Long,Int)]](2)
    //      for(i<-0 to 1)
    //        compute(i)=new ArrayBuffer[(Long, Int)]()
    //      grammar_match.getOrElse(EncodeLabel_pos(label,1),Array[(Int,Int)]())
    //        .foreach(s=> {
    //          val (x_f, label_A) = s
    //          compute(0).append((EncodeVid_labelpos(src, x_f), label_A))
    //        })
    //      grammar_match.getOrElse(EncodeLabel_pos(label,0),Array[(Int,Int)]()).foreach(s=>{
    //        val (y_b,label_B)=s
    //        compute(1).append((EncodeVid_labelpos(dst,y_b),label_B))
    //      })
    //      (ec,(update_src,update_dst),compute)
    //    }).persist(StorageLevel.MEMORY_ONLY_SER)
    //    println("newedges_count_update_compute.count: "+newedges_count_update_compute.count())
    //    //    println("newedges_count_update_compute: \n"+newedges_count_update_compute.collect().map(s=>(s._1+" "+s._2+" "+s._3.mkString(","))).mkString("\n"))
    //    /**
    //      * 3 Query Redis
    //      *   1) 形成 Query Redis 所需的所有键
    //      *   2) 分配到RDD的每条记录中
    //      */
    //    val newedges_count_update_compute_Answer:RDD[(((Int,Int,Int),Int),(Long,Long),Array[ArrayBuffer[(Long,Int)]],(java.util.Map[Integer,java.lang
    //    .Long],java.util.Map[Integer,java.lang.Long]),Array[Array[java.util.Map[Integer,java.lang.Long]]])
    //      ]=newedges_count_update_compute.mapPartitions(p_ecuc=>redis_OP.QueryAndUpdate_PB_RDD_Partition(p_ecuc)).persist
    //    (StorageLevel.MEMORY_ONLY_SER)
    //    println("newedges_count_update_compute_Answer.count: "+newedges_count_update_compute_Answer.count())
    //    //    println("newedges_count_update_compute_Answer: \n"+newedges_count_update_compute_Answer.collect().map(s=>(s._1+" " +
    //    //      ""+s._2+" "+s._3.mkString(",")+" "+s._4+" "+s._5(0).mkString(";")+" and "+s._5(1).mkString(";"))).mkString("\n"))
    //    //    val temp=newedges_count_update_compute_Answer.collect()
    //
    //    /**
    //      * 4 根据 Query_Answer 进行计算 和 count更新
    //      *   注: 若不存在原边则进行新边计算，所有的新边都要进行count更新
    //      */
    //
    //    /**
    //      * 4.2 计算新的边
    //      */
    //    /**
    //      * 4.2.1 计算新边与新边产生的
    //      */
    //    val newedges_nn:RDD[(Int,Int,Int)]=add_edges_count
    //      .flatMap(es=>Array((es._1._1,(EncodeLabel_pos(es._1._3,1),es._1._2)), (es._1._2, (EncodeLabel_pos(es._1._3,0),es._1._1))))
    //      .groupByKey()
    //      .flatMap(mid_edges=>{
    //        val res:ArrayBuffer[(Int,Int,Int)]=ArrayBuffer()
    //        val lable_pos_nodes:Map[Int,Array[Int]]=mid_edges._2.groupBy(_._1).map(s=>(s._1,s._2.map(e=>e._2).toArray))
    //        for(lpn<-lable_pos_nodes){
    //          if(DecodeLabel_pos(lpn._1)._2==0){//为防止重复，只以f为标准计算
    //          val other_b_target_label_arrays=grammar_match.getOrElse(lpn._1,null)
    //            if(other_b_target_label_arrays!=null){
    //              val srcs=lpn._2
    //              for((other_b,targetlabel)<-other_b_target_label_arrays){
    //                val dsts=lable_pos_nodes.getOrElse(other_b,Array[Int]())
    //                for(dst<-dsts){
    //                  for(src<-srcs){
    //                    res.append((src,dst,targetlabel))
    //                  }
    //                }
    //              }
    //            }
    //          }
    //        }
    //        res
    //      }).persist(StorageLevel.MEMORY_ONLY_SER)
    //    println("newedges_nn.count: \t"+newedges_nn.count())
    //    //    println("newedges_nn: \t"+newedges_nn.collect().mkString("\n"))
    //
    //    println("newedges_count_update_compute_Answer.count : "+newedges_count_update_compute_Answer.count())
    //    val newedges_on:RDD[(Int,Int,Int)]=
    //      newedges_count_update_compute_Answer.mapPartitions(p=>{
    //        val arrays=new ArrayBuffer[(Int,Int,Int)](1024)
    //        p.foreach(s=>{
    //          //          println(s._1)
    //          val src_label_b=s._2._1
    //          val src_label_b_map=s._4._1
    //          val (src,dst,label)=s._1._1
    //          //          println("src_label_b_map: "+src_label_b_map)
    //          if(src_label_b_map==null||src_label_b_map.getOrDefault(dst,null)==null){
    //            val res:ArrayBuffer[(Int,Int,Int)]=new ArrayBuffer[(Int, Int, Int)]()
    //            /**
    //              * for directadd
    //              */
    //            val label_add=directadd.getOrElse(label,-1)
    //            if(label_add!= -1) res.append((src,dst,label_add))
    //            /**
    //              * for grammar
    //              */
    //            val label_other_target=s._3
    //            /**
    //              * for src_x_f
    //              */
    //            var (src_x_f,label_A)=(0l,0)
    //            for(i<-0 to label_other_target(0).length-1){
    //              val uid_counts:java.util.Map[Integer,java.lang.Long]=s._5(0)(i)
    //              if(uid_counts!=null){
    //                src_x_f=label_other_target(0)(i)._1
    //                label_A=label_other_target(0)(i)._2
    //                val ks=getmapKeys(uid_counts)
    //                for(k<-ks) res.append((k, dst, label_A))
    //              }
    //            }
    //            /**
    //              * for dst_y_b
    //              */
    //            var (dst_y_b,label_B)=(0l,0)
    //            for(i<-0 to label_other_target(1).length-1){
    //              val uid_counts:java.util.Map[Integer,java.lang.Long]=s._5(1)(i)
    //              if(uid_counts!=null){
    //                dst_y_b=label_other_target(1)(i)._1
    //                label_B=label_other_target(1)(i)._2
    //                val ks=getmapKeys(uid_counts)
    //                for(k<-ks) res.append((src, k, label_B))
    //              }
    //            }
    //            println("res: "+res.length)
    //            arrays.appendAll(res)
    //          }
    //        })
    //        println("arrays: "+arrays.length)
    //        arrays.toIterator
    //      }).persist(StorageLevel.MEMORY_ONLY_SER)
    //    println("newedges_on.count: \t"+newedges_on.count())
    //    //    println("newedges_on: \t"+newedges_on.collect().mkString("\n"))
    //
    //    /**
    //      * 4.1 合并对同一个vid_label_pos的更新，此处不能直接应用map，不同的边可能会对同一个点进行更新
    //      */
    //    val UpdateCounts:RDD[(Long,java.util.Map[Integer,java.lang.Long])]=
    //    newedges_count_update_compute_Answer.flatMap(s=>Array((s._2._1,(s._1, s._4._1)),(s._2._2,(s._1,s._4._2))))
    //      .groupByKey()
    //      .map(s=>{
    //        val array=s._2.toArray
    //        (s._1,array(0)._2,array.map(_._1))
    //      })
    //      .mapPartitions(ss=>ss.map(s=>{
    //        val vid=DecodeVid_labelpos(s._1)._1
    //        val targetMap={
    //          if(s._2==null) new util.HashMap[Integer,java.lang.Long]()
    //          else s._2
    //        }
    //        var (count,src,dst,label)=(0,0,0,0)
    //        var encodedcounts=0L
    //        for(src_dst_label_count<-s._3){
    //          count=src_dst_label_count._2
    //          encodedcounts=EncodeCount(count,step)
    //          src=src_dst_label_count._1._1
    //          dst=src_dst_label_count._1._2
    //          label=src_dst_label_count._1._3
    //          if(src==vid) ProtocolBuffer_OP.UpdateCounts(targetMap,dst,encodedcounts)
    //          else ProtocolBuffer_OP.UpdateCounts(targetMap,src,encodedcounts)
    //        }
    //        (s._1,targetMap)
    //      }))
    //    println("UpdateCounts.count: "+UpdateCounts.count())
    //    //    println("UpdateCounts: "+UpdateCounts.collect().mkString("\n"))
    //    redis_OP.Update_PB(UpdateCounts)
    //    //    println("newedges_count_update_compute_Answer.count : "+newedges_count_update_compute_Answer.count())
    //    val res=newedges_nn.union(newedges_on).setName(s"add_edges_rdd $step").persist(StorageLevel.MEMORY_ONLY_SER)
    //    //    println(res.collect().mkString("\n"))
    //    res
  }


  def ComputeSingleMachine_ADD(add_edges:Array[(Int,Int,Int)],grammar_match:Map[Int,Array[(Int,Int)]],
                               directadd:Map[Int,Int],
                               step:Int,
                               redis_OP:Redis_OP):Array[(Int,Int,Int)]={
    /**
      * 1 统计数据
      */

    val add_edges_count_0:Array[((Int,Int,Int),Int)]=add_edges.map(e=>(e,1)).groupBy(_._1).map(eg=>(eg._1,eg._2
      .length)).toArray

    val add_edges_count_0_len=add_edges_count_0.length

    /**
      * 2 计算Update和filter所需的key，包括
      *   1) Update_Count所需的key : src_label_b and dst_label_f ，其中任选其一可作为filter
      *
      *   数据格式: (
      *     (src,dst,label),
      *     (src_label_,dst_label_f),
      *     Array[Arraybuffer[vid_label_pos,label_target]] : [(src_x_f,A)] and [(dst_y_b,B)]
      *     )
      */

    val key_Update_Filter:Array[(Long,Array[(Int,Long)])] = {
      add_edges_count_0.flatMap(ec=>{
        val ((src,dst,label),count)=ec
        val encodedcounts=EncodeCount(count,step)
        Array((EncodeVid_label_pos(src,label,1),(dst,encodedcounts)),(EncodeVid_label_pos(dst,label,0),(src,encodedcounts)))
      }).groupBy(_._1).map(s=>(s._1,s._2.map(_._2)))
    }.toArray

    val key_answer_counts_Update_Filter:Map[Long,(Array[(Int,Long)],util.Map[Integer,lang.Long])]=
      redis_OP.Query_PB_withCounts(key_Update_Filter)//MultiThread


    val add_edges_count_purenew:Array[(Int,Int,Int)]=add_edges_count_0.filter(s=>{
      val temp=key_answer_counts_Update_Filter.getOrElse(EncodeVid_label_pos(s._1._1,s._1._3,1),null)
      if(temp._2==null||temp._2.getOrDefault(s._1._2,null)==null) true
      else false
    }).map(_._1)
    //    println("add pure new : "+add_edges_count_purenew.length)
    flowadd+=add_edges_count_purenew.length



    /**
      * 3 Query Redis
      *   1) 形成 Compute 所需的所有键
      *   2) Query
      */
    val t0_3=System.nanoTime()

    // mid, Map[label_pos, Array[uid]]
    val mid_neighbour:Array[(Int,Map[Int,Array[Int]])]=add_edges_count_purenew.flatMap(s=>Array(
      (s._1,(EncodeLabel_pos(s._3,1),s._2)),(s._2,(EncodeLabel_pos(s._3,0),s._1))))
      .groupBy(_._1)
      .map(s=>(s._1,s._2.map(_._2).groupBy(_._1).map(x=>(x._1,x._2.map(_._2)))))
      .toArray


    val Query:Array[Long]={
      val longhashset=new LongOpenHashSet()
      mid_neighbour.foreach(s=>
        s._2.foreach(x=>{
          val other_label_poss=grammar_match.getOrElse(x._1,null)
          if(other_label_poss!=null){
            for(other_label_pos<-other_label_poss){
              for(uid<-x._2){
                longhashset.add(EncodeVid_labelpos(s._1,other_label_pos._1))
              }}}}))
      longhashset.toLongArray
    }

    val Query_Answer:Map[Long,java.util.Map[Integer,java.lang.Long]]=redis_OP.Query_PB_Array_Multithread(Query)//MuitiThread


    /**
      * 4 根据 Query_Answer 进行计算
      *
      *   new-new and new-old
      */
    val t0_4=System.nanoTime()
    var nn_num=0
    var no_num=0
    val add_edges_buffer:ArrayBuffer[(Int,Int,Int)]={//multithread
    val res:ArrayBuffer[(Int,Int,Int)]=new ArrayBuffer(add_edges_count_0_len)
      mid_neighbour.foreach(m_n=>{
        val mid=m_n._1
        m_n._2.foreach(label_pos_uids=>{
          val ((label,pos),uids)=(DecodeLabel_pos(label_pos_uids._1),label_pos_uids._2)
          val otherlabelpos_targetlabels=grammar_match.getOrElse(label_pos_uids._1,null)
          if(otherlabelpos_targetlabels!=null){
            /**
              * new-new
              */
            if(pos==0){//以f为准，避免重复计算
              for((otherlabelpos,targetlabel)<-otherlabelpos_targetlabels){
                val uid_dst=m_n._2.getOrElse(otherlabelpos,null)
                if(uid_dst!=null){
                  uids.foreach(src=>res.appendAll(uid_dst.map(dst=>(src,dst,targetlabel))))
                  nn_num+=uids.length*uid_dst.length
                }
              }
            }
            /**
              * new-old
              */
            for((otherlabelpos,targetlabel)<-otherlabelpos_targetlabels){
              val uid_old_count=Query_Answer.getOrElse(EncodeVid_labelpos(mid,otherlabelpos),null)
              if(uid_old_count!=null){
                val uid_old=getmapKeys(uid_old_count)
                if(pos==0){
                  uids.foreach(src=>res.appendAll(uid_old.map(dst=>(src,dst.toInt,targetlabel))))
                }//uids 在前
                else{
                  uids.foreach(dst=>res.appendAll(uid_old.map(src=>(src.toInt,dst,targetlabel))))
                }//uids 在后
                no_num+=uids.length*uid_old.length
              }}}
          /**
            * 产生 Mq::=M
            */
          if(pos==0) {
            val directadd_label = directadd.getOrElse(label, -1)
            if (directadd_label != -1)
              res.appendAll(uids.map(src => (src, mid, directadd_label)))
          }
        })})
      res
    }
    val t1_4=System.nanoTime()

    /**
      * 5 ArrayBuffer -> Array
      */
    val t0_5=System.nanoTime()
    val res=add_edges_buffer.toArray


    /**
      * 6 更新 Count
      * 数据格式： Map[Long,(Array[(Int,Long)],util.Map[Integer,lang.Long])]
      * Multithread
      */
    val t0_6=System.nanoTime()
    val len_Update=key_answer_counts_Update_Filter.size
    val keys2Update=new Array[Array[Byte]](len_Update)
    val values2Update=new Array[Array[Byte]](len_Update)
    var index=0
    for((k,v)<-key_answer_counts_Update_Filter) {
      keys2Update(index)=Long2ByteArray(k)
      val map={
        if(v._2==null) new util.HashMap[Integer,lang.Long]()
        else v._2
      }
      v._1.foreach(s=>ProtocolBuffer_OP.UpdateCounts(map,s._1,s._2))
      //      val encoded_counts = EncodeCount(count, step)
      //      //更新 dst_label_f_Map
      //        var temp=answer_Update_Filter(2*i+1)
      //        if(temp==null) temp=new java.util.HashMap[Integer, java.lang.Long]()
      //      ProtocolBuffer_OP.UpdateCounts(temp,src,encoded_counts)
      //      answer_Update_Filter(2*i+1)=temp
      //
      //      // 更新 src_label_b_Map
      //      temp=answer_Update_Filter(2*i)
      //      if(temp==null) temp=new java.util.HashMap[Integer, java.lang.Long]()
      //      ProtocolBuffer_OP.UpdateCounts(temp,dst,encoded_counts)
      //      answer_Update_Filter(2*i)=temp
      values2Update(index)=ProtocolBuffer_OP.Serialzed_Map_UidCounts(map)
      index+=1
    }
    val t1_6_1=System.nanoTime()

    // 正式更新
    redis_OP.Update_PB_ByteArray(keys2Update,values2Update)

    //    println(s"nn_num:\t$nn_num\tno_num:\t$no_num")
    res
  }



}

