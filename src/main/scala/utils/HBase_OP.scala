package utils

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.client.{Get, HBaseAdmin, HTable, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat, LoadIncrementalHFiles}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, KeyValue}
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer
/**
  * Created by cycy on 2018/1/22.
  */
object HBase_OP extends Para{

  val colum:String="e"
  val family:String="c"
  val value:String="1"

  def filling0(origin:Int,len:Int):String="0"*(len-origin.toString.length)+origin

  def getIntBit(a:Int):Int={
    var tmp=a
    var num=0
    while(tmp>0){
      tmp/=10
      num+=1
    }
    num
  }

  def Triple2String(src:Int,dst:Int,edgelabel:Int,nodes_num_bitsize:Int,symbol_num_bitsize:Int,htable_split_Map:Map[Int,String],
                    HRegion_splitnum:Int,default_split:String):String={
    val src_str = filling0(src, nodes_num_bitsize)
    val dst_str = filling0(dst, nodes_num_bitsize)
    val label_str = filling0(edgelabel, symbol_num_bitsize)
    htable_split_Map.getOrElse((src+dst) % HRegion_splitnum,default_split)+src_str + dst_str + label_str
    //    src_str + dst_str + label_str
  }

  def Edge2String(edge:(VertexId,VertexId,EdgeLabel),
                  nodes_num_bitsize:Int,symbol_num_bitsize:Int,htable_split_Map:Map[Int,String],
                  HRegion_splitnum:Int,default_split:String)
  :String={
    val src_str = filling0(edge._1, nodes_num_bitsize)
    val dst_str = filling0(edge._2, nodes_num_bitsize)
    val label_str = filling0(edge._3, symbol_num_bitsize)
    htable_split_Map.getOrElse((edge._1+edge._2) % HRegion_splitnum,default_split)+src_str + dst_str + label_str
    //    src_str + dst_str + label_str
  }

  def List2String(edge:List[Int],
                  nodes_num_bitsize:Int,symbol_num_bitsize:Int,htable_split_Map:Map[Int,String],
                  HRegion_splitnum:Int,default_split:String)
  :String={
    val src_str = filling0(edge(0), nodes_num_bitsize)
    val dst_str = filling0(edge(1), nodes_num_bitsize)
    val label_str = filling0(edge(2), symbol_num_bitsize)
    htable_split_Map.getOrElse((edge(0)+edge(1))% HRegion_splitnum,default_split)+src_str + dst_str + label_str
    //    src_str + dst_str + label_str
  }

  def Vector2String(edge:Vector[Int],
                    nodes_num_bitsize:Int,symbol_num_bitsize:Int,htable_split_Map:Map[Int,String],
                    HRegion_splitnum:Int,default_split:String)
  :String={
    val src_str = filling0(edge(0), nodes_num_bitsize)
    val dst_str = filling0(edge(1), nodes_num_bitsize)
    val label_str = filling0(edge(2), symbol_num_bitsize)
    htable_split_Map.getOrElse((edge(0)+edge(1)) % HRegion_splitnum,default_split)+src_str + dst_str + label_str
    //    src_str + dst_str + label_str
  }

  def Array2String(edge:Array[Int],
                   nodes_num_bitsize:Int,symbol_num_bitsize:Int,htable_split_Map:Map[Int,String],
                   HRegion_splitnum:Int,default_split:String)
  :String={
    val src_str = filling0(edge(0), nodes_num_bitsize)
    val dst_str = filling0(edge(1), nodes_num_bitsize)
    val label_str = filling0(edge(2), symbol_num_bitsize)
    htable_split_Map.getOrElse((edge(0)+edge(1)) % HRegion_splitnum,default_split)+src_str + dst_str + label_str
    //    src_str + dst_str + label_str
  }

  def createHBase_Table(htable_name:String,HRegion_splitnum:Int):(Map[Int,String],String) ={
    println("start create Hbase Table")
    /**
      *第1步：实例化HBaseAdmin
      */
    val h_conf = HBaseConfiguration.create()
    //    h_conf.set(TableOutputFormat.OUTPUT_TABLE, htable_name)
    h_conf.set("hbase.zookeeper.quorum", "slave001,slave002,slave003")
    val h_admin = new HBaseAdmin(h_conf)
    /**
      * 第2步：已有的table删除
      */
    if (h_admin.tableExists(htable_name)) {// 如果存在要创建的表，那么先删除，再创建
      h_admin.disableTable(htable_name)
      h_admin.deleteTable(htable_name)
      println(htable_name + " is exist,detele....")
    }
    /**
      * 第3步：创建TableDescriptor
      */
    //creating table descriptor
    val h_tableDesc = new HTableDescriptor(Bytes.toBytes(htable_name))
    //creating column family descriptor
    val family = new HColumnDescriptor(Bytes.toBytes(colum),1,"NONE",false,true,Int.MaxValue,"ROW")
    //adding coloumn family to HTable
    h_tableDesc.addFamily(family)
    /**
      * 3.1：预分区
      * 根据HRegion_splitnum，用Char的[0,...,HRegion_splitnum-1]区间内的字符作为分区的首字符
      */
    val region_splitnum_bitsize=getIntBit(HRegion_splitnum)
    val default_split:String=filling0(0,region_splitnum_bitsize)
    val split_str=(0 to HRegion_splitnum).toList.map(s=>filling0(s,region_splitnum_bitsize))
    val split_keys=split_str.map(s=>Bytes.toBytes(s)).toArray
    /**
      * 第4步：h_admin创建表
      */
    h_admin.createTable(h_tableDesc,split_keys)
    println("table "+htable_name+" created!")
    (split_str.zipWithIndex.map(s=>s.swap).toMap,default_split)
  }


  def queryHbase_inPartition(res_edges_maynotin:List[(VertexId,VertexId,EdgeLabel)],nodes_num_bitsize:Int,
                             symbol_num_bitsize:Int,htable_name:String,
                             htable_split_Map:Map[Int,String],htable_nodes_interval:Int,queryHbase_interval:Int,
                             default_split:String)
  :List[(VertexId,VertexId,EdgeLabel)]={
    val h_conf = HBaseConfiguration.create()
    h_conf.set("hbase.zookeeper.quorum", "slave001,slave002,slave003")
    //    println("hbase ipc.server.max.callqueue.size:\t"+h_conf.get("hbase ipc.server.max.callqueue.size"))
    h_conf.set("hbase ipc.server.max.callqueue.size","5368709120")
    //    println(h_conf.get("hbase ipc.server.max.callqueue.size"))
    h_conf.set(TableOutputFormat.OUTPUT_TABLE, htable_name)
    val h_table = new HTable(h_conf, htable_name)
    /**
      * Query interval
      */
    //        var res_all:List[(VertexId,VertexId,EdgeLabel)]=List()
    //        for(i:Int<-0 until (res_edges_maynotin.length,queryHbase_interval)){
    //          val sublist=res_edges_maynotin.subList(i,Math.min(i+queryHbase_interval,res_edges_maynotin.length))
    //          val g_l=sublist.map(x=>{
    //            val rk = Edge2String(x,nodes_num_bitsize,symbol_num_bitsize,htable_split_Map,htable_nodes_interval,default_split)
    //            val get = new Get(Bytes.toBytes(rk))
    //            get.setCheckExistenceOnly(true)
    //            get
    //          })
    //          val get_list_java: java.util.List[Get] = g_l
    //          val res_java = h_table.get(get_list_java)
    //          val res_list = res_java.map(x => x.getExists.booleanValue()).toList
    //          val res=(sublist zip res_list).filter(s => s._2 == false).map(s => s._1)
    //          res_all++=res
    //        }
    //        h_table.close()
    //        res_all
    /**
      * Query all
      */
    val get_list = res_edges_maynotin.map(x => {
      //      val rk = Edge2String(x,nodes_num_bitsize,symbol_num_bitsize,htable_split_Map,htable_nodes_interval,default_split)
      val get = new Get(Bytes.toBytes(Edge2String(x,nodes_num_bitsize,symbol_num_bitsize,htable_split_Map,htable_nodes_interval,default_split)))
      get.setCheckExistenceOnly(true)
      get
    })
    val get_list_java: java.util.List[Get] = get_list
    val res_java = h_table.get(get_list_java)
    val res_list = res_java.map(x => x.getExists.booleanValue()).toList
    val res=(res_edges_maynotin zip res_list).filter(s => s._2 == false).map(s => s._1)
    h_table.close()
    res
  }

  def queryHbase_inPartition_java_flat(res_edges_maynotin:Array[Array[Int]],nodes_num_bitsize:Int,
                                             symbol_num_bitsize:Int,
                                             Batch_QueryHbase:Boolean,htable_name:String,
                                             htable_split_Map:Map[Int,String],HRegion_splitnum:Int,queryHbase_interval:Int,
                                             default_split:String)
  :Array[Array[Int]]={
    val h_conf = HBaseConfiguration.create()
    h_conf.set("hbase.zookeeper.quorum", "slave001,slave002,slave003")
    //    println("hbase ipc.server.max.callqueue.size:\t"+h_conf.get("hbase ipc.server.max.callqueue.size"))
    h_conf.set("hbase.ipc.server.max.callqueue.size","5368709120")
    //    println(h_conf.get("hbase ipc.server.max.callqueue.size"))
    h_conf.set(TableOutputFormat.OUTPUT_TABLE, htable_name)
    val h_table = new HTable(h_conf, htable_name)

    val get_list_java = res_edges_maynotin.map(x => {
      val get = new Get(Bytes.toBytes(Array2String(x,nodes_num_bitsize,symbol_num_bitsize,htable_split_Map,
        HRegion_splitnum,default_split)))
      get.setCheckExistenceOnly(true)
      get
    }).toList

    val res={
      Batch_QueryHbase match{
        case true => {
          /**
            * Query interval
            */
          var res_all:List[Boolean]=List()
          for(i:Int<-0 until (res_edges_maynotin.length,queryHbase_interval)){
            val sublist=get_list_java.subList(i,Math.min(i+queryHbase_interval,res_edges_maynotin.length))
            val res_java = h_table.get(sublist)
            res_all = res_all.++(res_java.map(x => x.getExists.booleanValue()).toList)
          }
          (res_edges_maynotin zip res_all).filter(s => s._2 == false).map(s => s._1)
        }
        case false => {
          /**
            * Query all
            */
          val get_list = res_edges_maynotin.map(x => {
            //      val rk = Edge2String(x,nodes_num_bitsize,symbol_num_bitsize,htable_split_Map,HRegion_splitnum,default_split)
            val get = new Get(Bytes.toBytes(Array2String(x,nodes_num_bitsize,symbol_num_bitsize,htable_split_Map,
              HRegion_splitnum,default_split)))
            get.setCheckExistenceOnly(true)
            get
          })
          val get_list_java: java.util.List[Get] = get_list.toList
          val res_java = h_table.get(get_list_java)
          val res_list = res_java.map(x => x.getExists.booleanValue()).toList
          (res_edges_maynotin zip res_list).filter(s => s._2 == false).map(s => s._1)
        }
        case _ =>{
          var res_all:List[Boolean]=List()
          for(i:Int<-0 until (res_edges_maynotin.length,queryHbase_interval)){
            val sublist=get_list_java.subList(i,Math.min(i+queryHbase_interval,res_edges_maynotin.length))
            val res_java = h_table.get(sublist)
            res_all = res_all.++(res_java.map(x => x.getExists.booleanValue()).toList)
          }
          (res_edges_maynotin zip res_all).filter(s => s._2 == false).map(s => s._1)
        }
      }
    }
    h_table.close()
    res
  }

//  def queryHbase_inPartition_selfdecode(res_edges_maynotin:Array[Int],nodes_num_bitsize:Int,
//                                       symbol_num_bitsize:Int,
//                                       Batch_QueryHbase:Boolean,htable_name:String,
//                                       htable_split_Map:Map[Int,String],HRegion_splitnum:Int,queryHbase_interval:Int,
//                                       default_split:String)
//  :Array[Array[Int]]={
//    val h_conf = HBaseConfiguration.create()
//    h_conf.set("hbase.zookeeper.quorum", "slave001,slave002,slave003")
//    //    println("hbase ipc.server.max.callqueue.size:\t"+h_conf.get("hbase ipc.server.max.callqueue.size"))
//    h_conf.set("hbase.ipc.server.max.callqueue.size","5368709120")
//    //    println(h_conf.get("hbase ipc.server.max.callqueue.size"))
//    h_conf.set(TableOutputFormat.OUTPUT_TABLE, htable_name)
//    val h_table = new HTable(h_conf, htable_name)
//
//    val get_list_java = res_edges_maynotin.map(x => {
//      val get = new Get(Bytes.toBytes(Array2String(x,nodes_num_bitsize,symbol_num_bitsize,htable_split_Map,
//        HRegion_splitnum,default_split)))
//      get.setCheckExistenceOnly(true)
//      get
//    }).toList
//
//    val res={
//      Batch_QueryHbase match{
//        case true => {
//          /**
//            * Query interval
//            */
//          var res_all:List[Boolean]=List()
//          for(i:Int<-0 until (res_edges_maynotin.length,queryHbase_interval)){
//            val sublist=get_list_java.subList(i,Math.min(i+queryHbase_interval,res_edges_maynotin.length))
//            val res_java = h_table.get(sublist)
//            res_all = res_all.++(res_java.map(x => x.getExists.booleanValue()).toList)
//          }
//          (res_edges_maynotin zip res_all).filter(s => s._2 == false).map(s => s._1)
//        }
//        case false => {
//          /**
//            * Query all
//            */
//          val get_list = res_edges_maynotin.map(x => {
//            //      val rk = Edge2String(x,nodes_num_bitsize,symbol_num_bitsize,htable_split_Map,HRegion_splitnum,default_split)
//            val get = new Get(Bytes.toBytes(Array2String(x,nodes_num_bitsize,symbol_num_bitsize,htable_split_Map,
//              HRegion_splitnum,default_split)))
//            get.setCheckExistenceOnly(true)
//            get
//          })
//          val get_list_java: java.util.List[Get] = get_list.toList
//          val res_java = h_table.get(get_list_java)
//          val res_list = res_java.map(x => x.getExists.booleanValue()).toList
//          (res_edges_maynotin zip res_list).filter(s => s._2 == false).map(s => s._1)
//        }
//        case _ =>{
//          var res_all:List[Boolean]=List()
//          for(i:Int<-0 until (res_edges_maynotin.length,queryHbase_interval)){
//            val sublist=get_list_java.subList(i,Math.min(i+queryHbase_interval,res_edges_maynotin.length))
//            val res_java = h_table.get(sublist)
//            res_all = res_all.++(res_java.map(x => x.getExists.booleanValue()).toList)
//          }
//          (res_edges_maynotin zip res_all).filter(s => s._2 == false).map(s => s._1)
//        }
//      }
//    }
//    h_table.close()
//    res
//  }


  def queryHbase_compressnew_df(n:Array[Long],nodes_num_bitsize:Int, symbol_num_bitsize:Int,
                                Batch_QueryHbase:Boolean,htable_name:String,
                                htable_split_Map:Map[Int,String],HRegion_splitnum:Int,queryHbase_interval:Int,
                                default_split:String):Array[Array[Int]] ={
    val h_conf = HBaseConfiguration.create()
    h_conf.set("hbase.zookeeper.quorum", "slave001,slave002,slave003")
    //    println("hbase ipc.server.max.callqueue.size:\t"+h_conf.get("hbase ipc.server.max.callqueue.size"))
    h_conf.set("hbase.ipc.server.max.callqueue.size","5368709120")
    //    println(h_conf.get("hbase ipc.server.max.callqueue.size"))
    h_conf.set(TableOutputFormat.OUTPUT_TABLE, htable_name)
    val h_table = new HTable(h_conf, htable_name)

    val res:ArrayBuffer[Array[Int]]=new ArrayBuffer[Array[Int]](n.length*3)
    val get_list_java = {
      val tem=new Array[String](n.length)
      var i=0
      while(i<n.length){
        tem(i)=Triple2String((n(i) & 0xffffffffL).toInt,(n(i)>>>32).toInt, 0, nodes_num_bitsize,symbol_num_bitsize,
          htable_split_Map,HRegion_splitnum,default_split)
        i+=1
      }
      tem.map(s=>{
        val get = new Get(Bytes.toBytes(s))
        get.setCheckExistenceOnly(true)
        get
      }).toList
    }

      Batch_QueryHbase match{
        case true => {
          /**
            * Query interval
            */
          var res_all:List[Boolean]=List()
          for(i:Int<-0 until (n.length,queryHbase_interval)){
            val sublist=get_list_java.subList(i,Math.min(i+queryHbase_interval,n.length))
            val res_java = h_table.get(sublist)
            res_all = res_all.++(res_java.map(x => x.getExists.booleanValue()).toList)
          }
          var i=0
          val lenofres=res_all.length
          while(i<lenofres){
            if(res_all(i)==false) res.append(Array((n(i) & 0xffffffffL).toInt,(n(i)>>>32).toInt, 0))
            i+=1
          }
        }
        case false => {
          /**
            * Query all
            */

          val res_java = h_table.get(get_list_java)
          val res_list = res_java.map(x => x.getExists.booleanValue()).toList
          var i=0
          val lenofres=res_list.length
          while(i<lenofres){
            if(res_list(i)==false) res.append(Array((n(i) & 0xffffffffL).toInt,(n(i)>>>32).toInt, 0))
            i+=1
          }
        }
      }
    h_table.close()
    res.toArray
  }

  /**
    *
    * @param edge_processed
    * @param nodes_num_bitsize
    * @param symbol_num_bitsize
    * @param htable_name
    * @param output
    * @param htable_split_Map
    * @param HRegion_splitnum
    * @param default_split
    */
  def updateHbase(edge_processed:RDD[(VertexId,VertexId,EdgeLabel)],nodes_num_bitsize:Int,
                  symbol_num_bitsize:Int,htable_name:String,output:String,htable_split_Map:Map[Int,String],
                  HRegion_splitnum:Int,default_split:String)= {
    println("Update HBase Using BulkLoad")
    val h_conf = HBaseConfiguration.create()
    h_conf.set(TableOutputFormat.OUTPUT_TABLE, htable_name)
    h_conf.set("hbase.zookeeper.quorum", "slave001,slave002,slave003")
    h_conf.setInt("hbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily", 4096)
    val h_table =new HTable(h_conf, htable_name)
    val h_job = Job.getInstance(h_conf)
    h_job.setMapOutputKeyClass (classOf[ImmutableBytesWritable])
    h_job.setMapOutputValueClass (classOf[KeyValue])
    HFileOutputFormat.configureIncrementalLoad(h_job, h_table)

    edge_processed.map(s => {
      val final_string = Edge2String(s,nodes_num_bitsize,symbol_num_bitsize,htable_split_Map, HRegion_splitnum,default_split)
      val final_rowkey = Bytes.toBytes(final_string)
      val kv: KeyValue = new KeyValue(final_rowkey, colum.getBytes, family.getBytes, Bytes.toBytes(value))
      (final_string, (new ImmutableBytesWritable(final_rowkey), kv))
    }).sortByKey().map(s => s._2)
      .saveAsNewAPIHadoopFile(output, classOf[ImmutableBytesWritable],
        classOf[KeyValue],
        classOf[HFileOutputFormat], h_job.getConfiguration())

    val bulkLoader = new LoadIncrementalHFiles(h_job.getConfiguration())
    bulkLoader.doBulkLoad(new Path(output), h_table)
    h_table.close()
  }

  

  def updateHbase_java_flat(edge_processed:RDD[Array[Int]],nodes_num_bitsize:Int,
                            symbol_num_bitsize:Int,htable_name:String,output:String,htable_split_Map:Map[Int,String],
                            HRegion_splitnum:Int,default_split:String)= {
    println("Update HBase Using BulkLoad")
    var e:Exception=new Exception
    val h_conf = HBaseConfiguration.create()
    h_conf.set(TableOutputFormat.OUTPUT_TABLE, htable_name)
    h_conf.set("hbase.zookeeper.quorum", "slave001,slave002,slave003")
    h_conf.setInt("hbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily", 4096)
    val h_table =new HTable(h_conf, htable_name)
    val h_job = Job.getInstance(h_conf)
    h_job.setMapOutputKeyClass (classOf[ImmutableBytesWritable])
    h_job.setMapOutputValueClass (classOf[KeyValue])
    HFileOutputFormat.configureIncrementalLoad(h_job, h_table)

    edge_processed.map(s => {
      val final_string = Array2String(s,nodes_num_bitsize,symbol_num_bitsize,htable_split_Map, HRegion_splitnum,
        default_split)
      val final_rowkey = Bytes.toBytes(final_string)
      val kv: KeyValue = new KeyValue(final_rowkey, colum.getBytes, family.getBytes, Bytes.toBytes(value))
      (final_string, (new ImmutableBytesWritable(final_rowkey), kv))
    }).sortByKey().map(s => s._2)
      .saveAsNewAPIHadoopFile(output, classOf[ImmutableBytesWritable],
        classOf[KeyValue],
        classOf[HFileOutputFormat], h_job.getConfiguration())

    println("Bulkload")
    try {
      val bulkLoader = new LoadIncrementalHFiles(h_job.getConfiguration())
      bulkLoader.doBulkLoad(new Path(output), h_table)
    } catch {
      case e : Exception => {
        e.printStackTrace()
        throw new RuntimeException(e)
      }
    }
    h_table.close()
  }

  def updateHbase_batch(edge_processed:RDD[(VertexId,VertexId,EdgeLabel)],nodes_num_bitsize:Int,
                                symbol_num_bitsize:Int,htable_name:String,output:String,htable_split_Map:Map[Int,String],
                                HRegion_splitnum:Int,default_split:String,Hbase_interval:Int)= {
    println("Start Update HBase Partition")
    edge_processed.foreachPartition(s=> {
      val h_conf = HBaseConfiguration.create()
      h_conf.set("hbase.zookeeper.quorum", "slave001,slave002,slave003")
      val h_jobConf = new JobConf(h_conf, this.getClass)
      h_jobConf.setOutputFormat(classOf[TableOutputFormat])
      h_jobConf.set(TableOutputFormat.OUTPUT_TABLE, htable_name)
      val h_table =new HTable(h_conf, htable_name)

      val list = s.toList
      for (i: Int <- 0 until(list.length, Hbase_interval)) {
        val p_l = list.subList(i, Math.min(i + Hbase_interval, list.length)).map(x => {
          val p: Put = new Put(Bytes.toBytes(Edge2String(x,nodes_num_bitsize,symbol_num_bitsize,htable_split_Map,
            HRegion_splitnum,default_split)))
          p.add(colum.getBytes(), family.getBytes, Bytes.toBytes(value))
        })
        h_table.put(p_l)
      }
      h_table.close()
    })

    println("End Update HBase")
  }
}
