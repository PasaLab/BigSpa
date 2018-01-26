
/**
  * Created by cycy on 2018/1/18.
  * Grammar Format: A B C.---------->     A<-BC
  * InputGraph Format: src,dst,Label
  */

import java.util
import java.util.concurrent.Executors

import org.apache.hadoop.conf.Configuration
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.util.SizeEstimator
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client.{Get, HTable, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat, LoadIncrementalHFiles}
import HBase_OP._

object Graspan_noBF extends Para{

  def processGrammar(grammar_origin:List[Array[String]],input_grammar:String)
  :(Map[String,EdgeLabel],Int,Int,List[EdgeLabel],Map[EdgeLabel,EdgeLabel],List[((EdgeLabel,EdgeLabel),EdgeLabel)])={
    val symbol_Map=grammar_origin.flatMap(s=>s.toList).distinct.zipWithIndex.toMap
    val (loop:List[EdgeLabel],directadd:Map[EdgeLabel,EdgeLabel],grammar:List[((EdgeLabel,EdgeLabel),EdgeLabel)])={
      if(input_grammar.contains("pointsto")){
        println("Grammar need preprocessed")
        (grammar_origin.filter(s=>s.length==1).map(s=>symbol_Map.getOrElse(s(0),-1)),grammar_origin.filter(s=>s
          .length==2)
          .map(s=>(symbol_Map.getOrElse(s(1),-1),symbol_Map.getOrElse(s(0),-1))).toMap,grammar_origin.filter(s=>s
          .length==3).map(s=>((symbol_Map.getOrElse(s(1),-1),symbol_Map.getOrElse(s(2),-1)),symbol_Map.getOrElse(s(0)
          ,-1))))
      }
      else (List(),Map(),grammar_origin.map(s=>((symbol_Map.getOrElse(s(1),-1),symbol_Map.getOrElse(s(2),-1)),symbol_Map.getOrElse(s(0),-1))))
    }
    val symbol_num=symbol_Map.size
    val symbol_num_bitsize=getIntBit(symbol_num)
    println("In processGrammar")
    println("symbol_Map: "+symbol_Map.mkString("\t"))
    println("loop: "+loop.mkString("\t"))
    println("directadd: "+directadd.mkString("\t"))
    println("grammar_clean:\n"+grammar.mkString("\n"))
    println(" symbol_num_bitsize: "+symbol_num_bitsize)
    (symbol_Map,symbol_num,symbol_num_bitsize,loop,directadd,grammar)
  }
  def processGraph(graph_origin:RDD[(Int,Int,String)],input_grammar:String,symbol_Map:Map[String,EdgeLabel],
                   loop:List[EdgeLabel],
                   directadd:Map[EdgeLabel,EdgeLabel],par:Int):(RDD[(VertexId,VertexId,EdgeLabel)],Int,Int)={
    val graph_changelabel=graph_origin.map(s=>(s._1,s._2,symbol_Map.getOrElse(s._3,-1)))
    val nodes=graph_origin.flatMap(s=>List(s._1,s._2)).distinct().repartition(par)
    println("origin edges: "+graph_origin.count())
    val graph={
      if(input_grammar.contains("pointsto")){
        println("Graph need preprocessed")
        (graph_changelabel
          ++ nodes.flatMap(s=>loop.map(x=>(s,s,x)))
          ++ graph_changelabel.filter(s=>directadd.contains(s._3)).map(s=>(s._1,s._2,directadd.getOrElse(s._3,-1)))
          ).distinct()
      }
      else graph_origin.map(s=>(s._1,s._2,symbol_Map.getOrElse(s._3,-1))).distinct()
    }.cache()
    println("processed edges: "+graph.count())
    val nodes_totoalnum=nodes.count().toInt
    val nodes_num_bitsize=getIntBit(nodes_totoalnum)
    println("In processGraph")
    println("nodes_totoalnum: "+nodes_totoalnum)
    println("nodes_num_bitsize: "+nodes_num_bitsize)
    println("End processGraph")
    (graph,nodes_num_bitsize,nodes_totoalnum)
  }

  def join(mid_adj_list:List[(VertexId,List[((VertexId,VertexId),EdgeLabel,Boolean)])],
           grammar:List[((EdgeLabel, EdgeLabel),EdgeLabel)],
           directadd:Map[EdgeLabel,EdgeLabel])
  :(List[(VertexId,VertexId,EdgeLabel)],String)={
    val t0=System.nanoTime():Double
    var res_edges:List[(VertexId,VertexId,EdgeLabel)]=List()
    var tmp_str:String=""
    mid_adj_list.map(s=>{
      val flag=s._1
      val list=s._2
      val edge_before=list.filter(x=>x._1._2==flag).map(x=>(x._2,x._1._1,x._3)).groupBy(_._1)
        .map(x=>(x._1,x._2.map(y=>(y._2,y._3))))
      val edge_after=list.filter(x=>x._1._1==flag).map(x=>(x._2,x._1._2,x._3)).groupBy(_._1)
        .map(x=>(x._1,x._2.map(y=>(y._2,y._3))))
      for(i<-grammar){
        val (f,b)=i._1
        val res_label=i._2
        val f_list=edge_before.getOrElse(f,List())
        val b_list=edge_after.getOrElse(b,List())
        if(f_list.length<b_list.length){
          for(j<-f_list){
            if(j._2==false){
              res_edges ++= b_list.filter(x=>x._2).map(x=>(j._1,x._1,res_label)).distinct
            }
            else res_edges ++= b_list.map(x=>(j._1,x._1,res_label)).distinct
          }
        }
        else{
          for(j<-b_list){
            if(j._2==false){
              res_edges ++= f_list.filter(x=>x._2).map(x=>(x._1,j._1,res_label)).distinct
            }
            else res_edges ++= f_list.map(x=>(x._1,j._1,res_label)).distinct
          }
        }
      }
    })
    //MAM	AMs V DV
    //    tmp_str+="\nMAM: "+res_edges.filter(s=>s._3==5).length+", "
    //    tmp_str+="AMs: "+res_edges.filter(s=>s._3==6).length+", "
    //    tmp_str+="V: "+res_edges.filter(s=>s._3==4).length+", "
    //    tmp_str+="DV: "+res_edges.filter(s=>s._3==1).length+", "
    //    tmp_str+="M: "+res_edges.filter(s=>s._3==0).length+"\n"

    println("|| origin newedges: "+res_edges.length)
    tmp_str+="|| origin newedges: "+res_edges.length
    val add_edges=res_edges.filter(s=>directadd.contains(s._3)).map(s=>(s._1,s._2,directadd.getOrElse(s
      ._3,-1)))
    println("add_newedges: "+add_edges.length)
    tmp_str+=", add_newedges: "+add_edges.length
    res_edges=(res_edges ++ add_edges).distinct
    println("distinct newedges: "+res_edges.length+" ||")
    tmp_str+=", distinct newedges: "+res_edges.length+" ||"
    val t1=System.nanoTime():Double
    println("join take time:"+((t1-t0)/1000000000.0)+" secs\n")
    tmp_str+="join take time:"+(t1-t0)/1000000000.0 +" secs"
    (res_edges,tmp_str)
  }
  def computeInPartition_completely(step:Int,index:Int,
                                    mid_adj:Iterator[(VertexId,List[((VertexId,VertexId),EdgeLabel, Boolean)])],
                                    grammar:List[((EdgeLabel,EdgeLabel),EdgeLabel)],
                                    htable_name:String,
                                    nodes_num_bitsize:Int,symbol_num_bitsize:Int,
                                    directadd:Map[EdgeLabel,EdgeLabel],
                                    is_complete_loop:Boolean,max_complete_loop_turn:Int,max_delta:Int,
                                    htable_split_Map:Map[Int,String],
                                    htable_nodes_interval:Int,
                                    Hbase_interval:Int)
  :Iterator[(List[(VertexId,VertexId,EdgeLabel)],List[String])]={

    var t0=System.nanoTime():Double
    var t1=System.nanoTime():Double
    var recording:List[String]=List()
    val mid_adj_list=mid_adj.toList
    var old_edges:List[(VertexId,VertexId,EdgeLabel)]=mid_adj_list.flatMap(s=>(s._2)).map(s=>(s._1._1,s._1._2,s._2))
    println("At STEP "+step+", partition "+index+", old edges sum to "+old_edges.length)
    recording:+="IN partition "+index+", old edges sum to "+old_edges.length
    var (res_edges,tmp_str)=join(mid_adj_list,grammar,directadd)
    //    res_edges=res_edges.filter(s=> !old_edges.contains(s))
    println("res_edges distinct: "+res_edges.length)
    recording:+="res_edges distinct: "+res_edges.length
    /**
      * form clousure
      * only focused on edges from key inpartition or to key inpartition
      */
//    if(is_complete_loop){
//      val key_Set=mid_adj_list.map(s=>s._1).toSet
//      var continue:Boolean=is_complete_loop
//      var oldedges:List[(VertexId,VertexId,EdgeLabel,Boolean)]=old_edges.map(s=>(s._1,s._2,s._3,false))
//      var newedges:List[(VertexId,VertexId,EdgeLabel,Boolean)]=res_edges.map(s=>(s._1,s._2,s._3,true))
//      val first_new_num=newedges.length
//      val max_loop=max_complete_loop_turn
//      var turn=0
//      while(continue){
//        println("start loop ")
//        tmp_str+="start loop "
//        val t0=System.nanoTime():Double
//        turn+=1
//        val m_a_l=(oldedges ++ newedges).flatMap(s=>List((s._1,((s._1,s._2),s._3,s._4)),(s._2,((s._1,s._2),s._3,s._4)))
//        ).groupBy(_._1).toList.map(s=>(s._1,s._2.map(x=>x._2)))
//        val edges_before=(oldedges ++ newedges).map(s=>(s._1,s._2,s._3))
//        val (tmp_edges,tmp_str_inloop)=join(m_a_l,grammar,directadd)
//        tmp_str+=tmp_str_inloop
//        //      tmp_str+="bfore filter: "+tmp.length
//        oldedges=oldedges ++newedges.map(s=>(s._1,s._2,s._3,false))
//        //过滤新边，只保留与本partition有关的新边
//        newedges=tmp_edges.filter(s=>(key_Set.contains(s._1)||key_Set.contains(s._2))&&edges_before.contains(s)==false)
//          .map(s=>(s._1,s._2,s._3,true))
//        continue= (turn<max_loop && !newedges.isEmpty && oldedges.length-first_new_num<max_delta)
//        val t1=System.nanoTime():Double
//        println("complete_loop take time: "+((t1-t0)/ 1000000000.0/60).formatted("%.3f") + " min")
//        if(continue==false){
//          println("end loop")
//          tmp_str+="end loop"
//          recording:+=tmp_str
//          res_edges=oldedges.map(s=>(s._1,s._2,s._3))
//          println("after complete loop, res_edges= "+res_edges.length)
//          recording:+="after complete loop, res_edges= "+res_edges.length
//        }
//      }
//    }
    /**
      * 多线程开启
      */
//    val executors = Executors.newCachedThreadPool()
//    val thread = new MyThread
//    class MyThread extends Thread{
//      override def run(): Unit = {
//
//      }
//    }
//    executors.submit(thread)
    /**
      * Hbase过滤
      */
    t0=System.nanoTime():Double
    //    val distribution=res_edges.map(s=>(s._1,1)).groupBy(_._1).map(s=>(s._1,s._2.length)).toList.map(s=>(htable_split_Map
    //      .getOrElse(s
    //      ._1/htable_nodes_interval,"g"),s._2)).groupBy(_._1).map(s=>(s._1,s._2.map(x=>x._2).sum))
    //    println("res_edges distribution:")
    //    println(distribution.mkString("\n"))
    //    recording:+="res_edges distribution:"
    //    recording:+=distribution.mkString("\n")
    res_edges= {
      val res=queryHbase_inPartition(res_edges,nodes_num_bitsize,symbol_num_bitsize,htable_name,htable_split_Map,
        htable_nodes_interval,
        Hbase_interval)
      recording:+="res_edges confirmed new by Hbase: "+res.length
      res
    }
    t1=System.nanoTime():Double
    println("Query Hbase take time: "+((t1-t0)/ 1000000000.0).formatted("%.3f") + " sec")
    recording:+="Query Hbase take time: "+((t1-t0)/ 1000000000.0).formatted("%.3f") + " sec"

    println("res_edges: "+res_edges.length)
    recording:+="res_edges: "+res_edges.length
    List((res_edges,recording)).toIterator
  }

  def main(args: Array[String]): Unit = {
    var t0=System.nanoTime():Double
    var t1=System.nanoTime():Double
    var islocal: Boolean = true
    var master: String = "local"

    var input_grammar: String = "data/GrammarFiles/test_grammar"
    var input_graph:String="data/InputGraph/test_graph"
    var output: String = "data/result/" //除去ip地址
    var hbase_output:String="data/result/hbase/hbhfile"
    var par: Int = 96

    var openBloomFilter:Boolean=true
    var edges_totalnum:Int=1
    var error_rate:Double=0.1

    var htable_name:String="edges"
    var Hbase_interval:Int=1000
    var query_internal:Int=2

    var is_complete_loop:Boolean=false
    var max_complete_loop_turn:Int=5
    var max_delta:Int=10000

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
        case "par" => par = argvalue.toInt

        case "openBloomFilter"=>openBloomFilter=argvalue.toBoolean
        case "edges_totalnum"=>edges_totalnum=argvalue.toInt
        case "error_rate"=>error_rate=argvalue.toDouble

        case "htable_name"=>htable_name=argvalue
        case "Hbase_interval"=>Hbase_interval=argvalue.toInt
        case "query_internal"=>query_internal=argvalue.toInt

        case "is_complete_loop"=>is_complete_loop=argvalue.toBoolean
        case "max_complete_loop_turn"=>max_complete_loop_turn=argvalue.toInt
        case "max_delta"=>max_delta=argvalue.toInt

        case _ => {}
      }
    }

    /**
      * Spark 设置
      */
    val conf = new SparkConf().set("spark.kryoserializer.buffer.max", "1024")
    if (islocal) {
      //test location can be adjusted or not
      conf.setAppName("Graspan")
      System.setProperty("hadoop.home.dir", "F:/hadoop-2.6.0/")
      conf.setMaster("local")
    }
    val sc = new SparkContext(conf)

    /**
      * Hbase 设置
      */
    val h_conf = HBaseConfiguration.create()
    h_conf.set(TableOutputFormat.OUTPUT_TABLE, htable_name)
    h_conf.set("hbase.zookeeper.quorum", "slave001,slave002,slave003")
    h_conf.setInt("hbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily", 4096)
    val h_table =new HTable(h_conf, htable_name)
    val h_job = Job.getInstance(h_conf)
    h_job.setMapOutputKeyClass (classOf[ImmutableBytesWritable])
    h_job.setMapOutputValueClass (classOf[KeyValue])
    HFileOutputFormat.configureIncrementalLoad(h_job, h_table)

    /**
      * Grammar相关设置
      */
    val grammar_origin=sc.textFile(input_grammar).map(s=>s.split("\\s+")).collect().toList
    val (symbol_Map,symbol_num,symbol_num_bitsize,loop,directadd,grammar)=processGrammar(grammar_origin,input_grammar)

    /**
      * Graph相关设置
      */
    val graph_origin=sc.textFile(input_graph,par).filter(!_.trim.equals("")).map(s=>{
      val str=s.split("\\s+")
      (str(0).toInt,str(1).toInt,str(2))
    })
    if(graph_origin.isEmpty()){
      println("input graph is empty")
      System.exit(0)
    }
    val(graph,nodes_num_bitsize,nodes_totalnum)=processGraph(graph_origin,input_grammar,symbol_Map,loop,
      directadd,par)
    graph_origin.unpersist()
    val split=List("0","1","2","3","4","5","6","7","8","9","a","b","c","d","e","f","g","h","i","j","k","l","m","n",
      "o","p","q","r","s","t","u","v","w","x","y","z","A")
    val htable_split_Map:Map[Int,String]=split.zipWithIndex.map(s=>s.swap).toMap
    val htable_nodes_interval:Int=nodes_totalnum/36

    /**
      * 原边集存入Hbase
      */
    println("graph Partitions: "+graph.partitions.length)
    deleteDir.deletedir(islocal,master,hbase_output)
    updateHbase(graph,nodes_num_bitsize,symbol_num_bitsize,h_conf,h_job,h_table,hbase_output,
      htable_split_Map,htable_nodes_interval)
    //    updateHbase_new_Partition(graph.repartition(par),nodes_num_bitsize,symbol_num_bitsize,htable_name,
    //      output+"/tmp/iteblog",
    //          htable_split_Map,htable_nodes_interval,Hbase_interval)

    /**
      * 开始迭代
      */
    var oldedges:RDD[(VertexId,VertexId,EdgeLabel,Boolean)]=sc.parallelize(List())
    var newedges:RDD[(VertexId,VertexId,EdgeLabel,Boolean)]=graph.map(s=>(s._1,s._2,s._3,true)).cache()
    graph.unpersist()
    var step=0
    var continue:Boolean= !newedges.isEmpty()
    while(continue){
      t0=System.nanoTime():Double
      step+=1
      println("\n************During step "+step+"************")
      //      println("old edges: "+oldedges.count+"new edges: "+newedges.count)
      //      println(newedges.collect().mkString("\t"))
      val t0_compute=System.nanoTime():Double
      val newedges_dup_str=(oldedges ++ newedges).flatMap(s=>List((s._1,((s._1,s._2),s._3,s._4)),(s._2,
        ((s._1,s._2),s._3,s._4))))
        .groupByKey()
        .map(s=>(s._1,s._2.toList))
        .repartition(par)
        .mapPartitionsWithIndex((index,s)=>computeInPartition_completely(step,index,s,grammar,
          htable_name,
          nodes_num_bitsize,symbol_num_bitsize,directadd,
          is_complete_loop,max_complete_loop_turn,max_delta,htable_split_Map,htable_nodes_interval,Hbase_interval))
        .cache()

      println("new_edges_bf count inPartition: \n")
      println(newedges_dup_str.map(s=>s._2).collect().mkString("\n"))
      val t1_compute=System.nanoTime():Double
      println("clousure compute take time= "+((t1_compute-t0_compute) / 1000000000.0).formatted("%.3f") + " sec")

      /**
        * 获得各分区内经过Bloom Filter和Hbase过滤之后留下的新边
        * 再汇总distinct
        */
      val t0_distinct=System.nanoTime():Double
      val newedges_dup=newedges_dup_str.flatMap(s=>s._1)
      println("newedges_dup: "+newedges_dup.count())
      val newedges_removedup=newedges_dup.distinct()
      deleteDir.deletedir(islocal,master,output+"step"+step)

      newedges_dup.map(s=>(s._1+"\t"+s._2+"\t"+s._3)).repartition(1).saveAsTextFile(output+"step"+step)

      println("newedges_removedup:"+newedges_removedup.count())
      //      println(newedges_removedup.collect().mkString("\n"))
      val t1_distinct=System.nanoTime():Double
      println("distinct take time= "+((t1_distinct-t0_distinct) / 1000000000.0).formatted("%.3f") + " sec")

      newedges_dup_str.unpersist()

      /**
        * Hbase更新
        */
      val t0_hb=System.nanoTime():Double
      deleteDir.deletedir(islocal,master,hbase_output)
      updateHbase(newedges_removedup,nodes_num_bitsize,symbol_num_bitsize,h_conf,h_job,h_table,hbase_output,
        htable_split_Map,htable_nodes_interval)
      //      updateHbase_new_Partition(newedges_removedup,nodes_num_bitsize,symbol_num_bitsize,htable_name,
      //        output+"/tmp/iteblog",
      //        htable_split_Map,htable_nodes_interval,Hbase_interval)
      val t1_hb=System.nanoTime():Double
      println("update Hbase take time= "+((t1_hb-t0_hb) / 1000000000.0).formatted("%.3f") + " sec")
      val tmp_old = oldedges
      val tmp_new = newedges
      oldedges=(oldedges ++ newedges.map(s=>(s._1,s._2,s._3,false))).repartition(par).cache()
      newedges=newedges_removedup.map(s=>(s._1,s._2,s._3,true)).cache()
      println("all edges sum to "+oldedges.count())
      continue= !(newedges.isEmpty())
      t1=System.nanoTime():Double
      tmp_old.unpersist()
      tmp_new.unpersist()
      println("step "+step+" take time= "+((t1 - t0) / 1000000000.0/60).formatted("%.3f") + " min")
    }

    println("final edges count "+oldedges.count())
    h_table.close()
  }

}
