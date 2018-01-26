import java.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.{HBaseConfiguration, KeyValue}
import org.apache.hadoop.hbase.client.{Get, HTable, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat, LoadIncrementalHFiles}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD
import scala.collection.JavaConversions._
/**
  * Created by cycy on 2018/1/22.
  */
object HBase_OP extends Para{

  def filling0(origin:Int,len:Int):String={
    val str=origin.toString
    val lenof0=len-str.length
    val filling=new Array[Int](lenof0)
    filling.mkString("") + str
  }
  def getIntBit(a:Int):Int={
    var tmp=a
    var num=0
    while(tmp>0){
      tmp/=10
      num+=1
    }
    num
  }
  def Edge2String(edge:(VertexId,VertexId,EdgeLabel),
                  nodes_num_bitsize:Int,symbol_num_bitsize:Int,htable_split_Map:Map[Int,String],
                  htable_nodes_interval:Int)
  :String={
    val src_str = filling0(edge._1, nodes_num_bitsize)
    val dst_str = filling0(edge._2, nodes_num_bitsize)
    val label_str = filling0(edge._3, symbol_num_bitsize)
    htable_split_Map.getOrElse(edge._1/htable_nodes_interval,"A")+src_str + dst_str + label_str
//    src_str + dst_str + label_str
  }

  def queryHbase_inPartition(res_edges_maynotin:List[(VertexId,VertexId,EdgeLabel)],nodes_num_bitsize:Int,
                             symbol_num_bitsize:Int,htable_name:String,htable_split_Map:Map[Int,String],
                             htable_nodes_interval:Int,Hbase_interval:Int):List[(VertexId,VertexId,EdgeLabel)]={

    val h_conf = HBaseConfiguration.create()
    h_conf.set("hbase.zookeeper.quorum", "slave001,slave002,slave003")
    h_conf.set(TableOutputFormat.OUTPUT_TABLE, htable_name)
    val h_table = new HTable(h_conf, htable_name)

    var res_all:List[(VertexId,VertexId,EdgeLabel)]=List()
    for(i:Int<-0 until (res_edges_maynotin.length,Hbase_interval)){
      val sublist=res_edges_maynotin.subList(i,Math.min(i+Hbase_interval,res_edges_maynotin.length))
      val g_l=sublist.map(x=>{
        val rk = Edge2String(x,nodes_num_bitsize,symbol_num_bitsize,htable_split_Map,htable_nodes_interval)
        val get = new Get(Bytes.toBytes(rk))
        get.setCheckExistenceOnly(true)
        get
      })
      val get_list_java: java.util.List[Get] = g_l
      val res_java = h_table.get(get_list_java)
      val res_list = res_java.map(x => x.getExists.booleanValue()).toList
      val res=(sublist zip res_list).filter(s => s._2 == false).map(s => s._1)
      res_all++=res
    }
    h_table.close()
    res_all

//    val get_list = res_edges_maynotin.map(x => {
//      val rk = Edge2String(x,nodes_num_bitsize,symbol_num_bitsize,htable_split_Map,htable_nodes_interval)
//      val get = new Get(Bytes.toBytes(rk))
//      get.setCheckExistenceOnly(true)
//      get
//    })
//    val get_list_java: java.util.List[Get] = get_list
//    val res_java = h_table.get(get_list_java)
//    val res_list = res_java.map(x => x.getExists.booleanValue()).toList
//    val res=(res_edges_maynotin zip res_list).filter(s => s._2 == false).map(s => s._1)
//    h_table.close()
//    res
  }

  def updateHbase(edge_processed:RDD[(VertexId,VertexId,EdgeLabel)],nodes_num_bitsize:Int,
                  symbol_num_bitsize:Int,h_conf:Configuration,h_job:Job,h_table:HTable,output:String,htable_split_Map:Map[Int,String],
                  htable_nodes_interval:Int)= {
    println("Update HBase Using BulkLoad")
    edge_processed.map(s => {
      val final_string = Edge2String(s,nodes_num_bitsize,symbol_num_bitsize,htable_split_Map, htable_nodes_interval)
      val final_rowkey = Bytes.toBytes(final_string)
      val read_string = "1"
      val kv: KeyValue = new KeyValue(final_rowkey, "edge".getBytes, "contents".getBytes, Bytes.toBytes(read_string))
      (final_string, (new ImmutableBytesWritable(final_rowkey), kv))
    }).sortByKey().map(s => s._2)
      .saveAsNewAPIHadoopFile(output, classOf[ImmutableBytesWritable],
        classOf[KeyValue],
        classOf[HFileOutputFormat], h_job.getConfiguration())

    val bulkLoader = new LoadIncrementalHFiles(h_job.getConfiguration())
    bulkLoader.doBulkLoad(new Path(output), h_table)
  }

//  def updateHbase_new(edge_processed:RDD[(VertexId,VertexId,EdgeLabel)],nodes_num_bitsize:Int,
//                      symbol_num_bitsize:Int,htable_name:String,output:String,htable_split_Map:Map[Int,String],
//                      htable_nodes_interval:Int)= {
//    println("Start Update HBase")
//    val h_conf = HBaseConfiguration.create()
//    h_conf.set("hbase.zookeeper.quorum", "slave001,slave002,slave003")
//    val h_jobConf = new JobConf(h_conf,this.getClass)
//    h_jobConf.setOutputFormat(classOf[TableOutputFormat])
//    h_jobConf.set(TableOutputFormat.OUTPUT_TABLE,htable_name)
//
//    edge_processed.map(s => {
//      val p:Put = new Put(Bytes.toBytes(Edge2String(s,nodes_num_bitsize,symbol_num_bitsize,htable_split_Map,
//        htable_nodes_interval)))
//      p.add("edge".getBytes(), "contents".getBytes, Bytes.toBytes("1"))
//      (new ImmutableBytesWritable, p)
//    }).saveAsHadoopDataset(h_jobConf)
//    println("End Update HBase")
//  }
//
//  def updateHbase_new_Partition(edge_processed:RDD[(VertexId,VertexId,EdgeLabel)],nodes_num_bitsize:Int,
//                                symbol_num_bitsize:Int,htable_name:String,output:String,htable_split_Map:Map[Int,String],
//                                htable_nodes_interval:Int,Hbase_interval:Int)= {
//    println("Start Update HBase Partition")
//    edge_processed.foreachPartition(s=> {
//      var lp: util.ArrayList[Put] = new util.ArrayList[Put]()
//      val h_conf = HBaseConfiguration.create()
//      h_conf.set("hbase.zookeeper.quorum", "slave001,slave002,slave003")
//      val h_jobConf = new JobConf(h_conf, this.getClass)
//      h_jobConf.setOutputFormat(classOf[TableOutputFormat])
//      h_jobConf.set(TableOutputFormat.OUTPUT_TABLE, htable_name)
//      val h_table =new HTable(h_conf, htable_name)
//
//      val list = s.toList
//      for (i: Int <- 0 until(list.length, Hbase_interval)) {
//        val p_l = list.subList(i, Math.min(i + Hbase_interval, list.length)).map(x => {
//          val p: Put = new Put(Bytes.toBytes(Edge2String(x, nodes_num_bitsize, symbol_num_bitsize, htable_split_Map,
//            htable_nodes_interval)))
//          p.add("edge".getBytes(), "contents".getBytes, Bytes.toBytes("1"))
//        })
//        h_table.put(p_l)
//      }
//      h_table.close()
//    })
//
//    println("End Update HBase")
//  }
}
