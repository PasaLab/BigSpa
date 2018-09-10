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
class HBase_OP extends Para with DataBase_OP with Serializable{

  val colum:String="e"
  val family:String="c"
  val value:String="1"

  var hbase_output:String=""
  var nodes_num_bitsize:Int=0
  var symbol_num_bitsize:Int=0
  var Batch_QueryHbase:Boolean=true
  var htable_name:String="edges"
  var HRegion_splitnum:Int=1
  var queryHbase_interval:Int=50000

  var region_splitnum_bitsize:Int=1

  def this(_hbase_output:String,
               _nodes_num_bitsize:Int,
               _symbol_num_bitsize:Int=0,
               _Batch_QueryHbase:Boolean=false,
               _htable_name:String="edges",
               _HRegion_splitnum:Int=0,
               _queryHbase_interval:Int=50000) {
    this()
    hbase_output=_hbase_output
    nodes_num_bitsize=_nodes_num_bitsize
    symbol_num_bitsize=_symbol_num_bitsize
    Batch_QueryHbase=_Batch_QueryHbase
    htable_name=_htable_name
    HRegion_splitnum=_HRegion_splitnum
    queryHbase_interval=_queryHbase_interval
  }
  var default_split:String=""
  var htable_split_Map:Map[Int,String]=null

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

//  def Triple2String(src:Int,dst:Int,edgelabel:Int):String={
//    val src_str = filling0(src, nodes_num_bitsize)
//    val dst_str = filling0(dst, nodes_num_bitsize)
//    val label_str = filling0(edgelabel, symbol_num_bitsize)
//    htable_split_Map.getOrElse((src+dst) % HRegion_splitnum,default_split)+src_str + dst_str + label_str
//    //    src_str + dst_str + label_str
//  }

//  def Edge2String(edge:(VertexId,VertexId,EdgeLabel))
//  :String={
//    val src_str = filling0(edge._1, nodes_num_bitsize)
//    val dst_str = filling0(edge._2, nodes_num_bitsize)
//    val label_str = filling0(edge._3, symbol_num_bitsize)
//    htable_split_Map.getOrElse((edge._1+edge._2) % HRegion_splitnum,default_split)+src_str + dst_str + label_str
//    //    src_str + dst_str + label_str
//  }

//  def List2String(edge:List[Int])
//  :String={
//    val src_str = filling0(edge(0), nodes_num_bitsize)
//    val dst_str = filling0(edge(1), nodes_num_bitsize)
//    val label_str = filling0(edge(2), symbol_num_bitsize)
//    htable_split_Map.getOrElse((edge(0)+edge(1))% HRegion_splitnum,default_split)+src_str + dst_str + label_str
//    //    src_str + dst_str + label_str
//  }
//
//  def Vector2String(edge:Vector[Int])
//  :String={
//    val src_str = filling0(edge(0), nodes_num_bitsize)
//    val dst_str = filling0(edge(1), nodes_num_bitsize)
//    val label_str = filling0(edge(2), symbol_num_bitsize)
//    htable_split_Map.getOrElse((edge(0)+edge(1)) % HRegion_splitnum,default_split)+src_str + dst_str + label_str
//    //    src_str + dst_str + label_str
//  }
//
  def Array2String(edge:Array[Int])
  :String={
    val src_str = filling0(edge(0), nodes_num_bitsize)
    val dst_str = filling0(edge(1), nodes_num_bitsize)
    val label_str = filling0(edge(2), symbol_num_bitsize)
    htable_split_Map.getOrElse((edge(0)+edge(1)) % HRegion_splitnum,default_split)+src_str + dst_str + label_str
    //    src_str + dst_str + label_str
  }

  def Triple2String(src:Int,dst:Int,label:Int)
  :String={
    val src_str = filling0(src, nodes_num_bitsize)
    val dst_str = filling0(dst, nodes_num_bitsize)
    val label_str = filling0(label, symbol_num_bitsize)
    htable_split_Map.getOrElse((src+dst) % HRegion_splitnum,default_split)+src_str + dst_str + label_str
    //    src_str + dst_str + label_str
  }

//  def Triple2ArrayByte(src:Int,dst:Int,edgelabel:Int):Array[Byte]={
//    val res=new Array[Byte](9)
//    res(0)=((src >> 24) & 0xFF).toByte
//    res(1)=((src >> 16) & 0xFF).toByte
//    res(2)=((src >> 8) & 0xFF).toByte
//    res(3)=(src & 0xFF).toByte
//
//    res(4)=((dst >> 24) & 0xFF).toByte
//    res(5)=((dst >> 16) & 0xFF).toByte
//    res(6)=((dst >> 8) & 0xFF).toByte
//    res(7)=(dst & 0xFF).toByte
//
//    res(8)=(edgelabel & 0xFF).toByte
//
//    res
//  }

//  def ArrayByte2ArrayInt(s:Array[Byte]):Array[Int]= {
////    println("HBase 的 get 返回的row 字节数为 "+s.length)
//    val array=new Array[Int](3)
//    array(0)=(s(0).toInt << 24 )+ (s(1).toInt << 16) +(s(2).toInt << 8)+ s(3)
//    array(1)=(s(4).toInt << 24 )+ (s(5).toInt << 16) +(s(6).toInt << 8)+ s(7)
//    array(2)=s(8)
//    array
//  }


  def createHBase_Table(){
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
    region_splitnum_bitsize=getIntBit(HRegion_splitnum)
    default_split=filling0(0,region_splitnum_bitsize)
    val split_str=(0 to HRegion_splitnum).toList.map(s=>filling0(s,region_splitnum_bitsize))
    val split_keys=split_str.map(s=>Bytes.toBytes(s)).toArray
    /**
      * 第4步：h_admin创建表
      */
    h_admin.createTable(h_tableDesc,split_keys)
    println("table "+htable_name+" created!")
    htable_split_Map=split_str.zipWithIndex.map(s=>s.swap).toMap
  }


  /**
    * Query Operation
    * @return
    */
  def Query_PT(compressed_edges: Array[Int])
  :Array[Array[Int]]={
    val h_conf = HBaseConfiguration.create()
    h_conf.set("hbase.zookeeper.quorum", "slave001,slave002,slave003")
    //    println("hbase ipc.server.max.callqueue.size:\t"+h_conf.get("hbase ipc.server.max.callqueue.size"))
    h_conf.set("hbase.ipc.server.max.callqueue.size","5368709120")
    //    println(h_conf.get("hbase ipc.server.max.callqueue.size"))
    h_conf.set(TableOutputFormat.OUTPUT_TABLE, htable_name)
    val h_table = new HTable(h_conf, htable_name)

    val all_edges_num: Int = compressed_edges.length / 3
    val keys: Array[Get] = new Array[Get](all_edges_num)
    val res:ArrayBuffer[Array[Int]]=new ArrayBuffer[Array[Int]](all_edges_num)
    var i: Int = 0
    while (i < all_edges_num) {
        keys(i) =new Get(Bytes.toBytes(Triple2String(compressed_edges(i*3),compressed_edges(i*3+1),compressed_edges
        (i*3+2))))
          .setCheckExistenceOnly(true)
        i += 1
    }

      Batch_QueryHbase match{
        case true => {
          /**
            * Query interval
            */

          for(i:Int<-0 until (all_edges_num,queryHbase_interval)){
            val sublist=keys.slice(i,Math.min(i+queryHbase_interval,all_edges_num)).toList
            val res_java = h_table.get(sublist).map(x => x.getExists.booleanValue())
            var index=0
            val len=res_java.length
            while(index<len){
              if(!res_java(index)){
                val order=(i+index)*3
                res.append(Array(compressed_edges(order),compressed_edges(order+1),compressed_edges(order+2)))
              }
              index+=1
            }
          }
        }
        case false => {
          val res_java = h_table.get(keys.toList).map(x => x.getExists.booleanValue())
          var index=0;
          val len=res_java.length
          while(index<len){
            if(!res_java(index)){
              val order=index*3
              res.append(Array(compressed_edges(order),compressed_edges(order+1),compressed_edges(order+2)))
            }
            index+=1
          }
        }
      }

    h_table.close()
    res.toArray
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

  def Query_DF(n:Array[Long]):Array[Array[Int]] ={
    val h_conf = HBaseConfiguration.create()
    h_conf.set("hbase.zookeeper.quorum", "slave001,slave002,slave003")
    //    println("hbase ipc.server.max.callqueue.size:\t"+h_conf.get("hbase ipc.server.max.callqueue.size"))
    h_conf.set("hbase.ipc.server.max.callqueue.size","5368709120")
    //    println(h_conf.get("hbase ipc.server.max.callqueue.size"))
    h_conf.set(TableOutputFormat.OUTPUT_TABLE, htable_name)
    val h_table = new HTable(h_conf, htable_name)


    val all_edges_num: Int = n.length
    val keys: Array[Get] = new Array[Get](all_edges_num)
    val res:ArrayBuffer[Array[Int]]=new ArrayBuffer[Array[Int]](all_edges_num)
    var i: Int = 0
    while (i < all_edges_num) {
      keys(i) =new Get(Bytes.toBytes(Triple2String((n(i) & 0xffffffffL).toInt,(n(i)>>>32).toInt, 0)))
        .setCheckExistenceOnly(true)
      i += 1
    }

    Batch_QueryHbase match{
      case true => {
        /**
          * Query interval
          */
        for(i:Int<-0 until (all_edges_num,queryHbase_interval)){
          val sublist=keys.slice(i,Math.min(i+queryHbase_interval,all_edges_num)).toList
          val res_java = h_table.get(sublist).map(x => x.getExists.booleanValue())
          var index=0;
          val len=res_java.length
          while(index<len){
            if(!res_java(index)){
              val order=(i+index)
              res.append(Array((n(order) & 0xffffffffL).toInt,(n(order)>>>32).toInt,0))
            }
            index+=1
          }
        }
      }
      case false => {
        val res_java = h_table.get(keys.toList).map(x => x.getExists.booleanValue())
        var index=0;
        val len=res_java.length
        while(index<len){
          if(!res_java(index)){
            res.append(Array((n(index) & 0xffffffffL).toInt,(n(index)>>>32).toInt, 0))
          }
          index+=1
        }
      }
    }

    h_table.close()
    res.toArray
  }

  /**
    *Update Operation
    * @param edge_processed
    */

  def Update(edge_processed:RDD[Array[Int]])= {
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
      val final_String=Array2String(s)
      val final_rowkey = Bytes.toBytes(final_String)
      val kv: KeyValue = new KeyValue(final_rowkey, colum.getBytes, family.getBytes, Bytes.toBytes(value))
      (final_String, (new ImmutableBytesWritable(final_rowkey), kv))
    }).sortByKey().map(s => s._2)
      .saveAsNewAPIHadoopFile(hbase_output, classOf[ImmutableBytesWritable],
        classOf[KeyValue],
        classOf[HFileOutputFormat], h_job.getConfiguration())

    println("Bulkload")
    try {
      val bulkLoader = new LoadIncrementalHFiles(h_job.getConfiguration())
      bulkLoader.doBulkLoad(new Path(hbase_output), h_table)
    } catch {
      case e : Exception => {
        e.printStackTrace()
        throw new RuntimeException(e)
      }
    }
    h_table.close()
  }

//  def updateHbase_batch(edge_processed:RDD[(VertexId,VertexId,EdgeLabel)])= {
//    println("Start Update HBase Partition")
//    edge_processed.foreachPartition(s=> {
//      val h_conf = HBaseConfiguration.create()
//      h_conf.set("hbase.zookeeper.quorum", "slave001,slave002,slave003")
//      val h_jobConf = new JobConf(h_conf, this.getClass)
//      h_jobConf.setOutputFormat(classOf[TableOutputFormat])
//      h_jobConf.set(TableOutputFormat.OUTPUT_TABLE, htable_name)
//      val h_table =new HTable(h_conf, htable_name)
//
//      val list = s.toList
//      for (i: Int <- 0 until(list.length,updateHbase_interval)) {
//        val p_l = list.subList(i, Math.min(i + updateHbase_interval, list.length)).map(x => {
//          val p: Put = new Put(Bytes.toBytes(Edge2String(x)))
//          p.add(colum.getBytes(), family.getBytes, Bytes.toBytes(value))
//        })
//        h_table.put(p_l)
//      }
//      h_table.close()
//    })
//    println("End Update HBase")
//  }
}
