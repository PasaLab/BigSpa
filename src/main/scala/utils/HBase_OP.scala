package utils

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.client.{Get, HBaseAdmin, HTable}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat, LoadIncrementalHFiles}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, KeyValue}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD

import scala.collection.JavaConversions._
/**
  * Created by cycy on 2018/1/22.
  */
object HBase_OP extends Para{

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

  def Edge2String(edge:(VertexId,VertexId,EdgeLabel),
                  nodes_num_bitsize:Int,symbol_num_bitsize:Int,htable_split_Map:Map[Int,String],
                  htable_nodes_interval:Int,default_split:String)
  :String={
    val src_str = filling0(edge._1, nodes_num_bitsize)
    val dst_str = filling0(edge._2, nodes_num_bitsize)
    val label_str = filling0(edge._3, symbol_num_bitsize)
    htable_split_Map.getOrElse(edge._1/htable_nodes_interval,default_split)+src_str + dst_str + label_str
    //    src_str + dst_str + label_str
  }

  def createHBase_Table(htable_name:String,HRegion_splitnum:Int):(Map[Int,String],String) ={
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
    val family = new HColumnDescriptor(Bytes.toBytes("edges"),1,"NONE",false,true,Int.MaxValue,"ROW")
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
                             htable_split_Map:Map[Int,String],htable_nodes_interval:Int,Hbase_interval:Int,
                             default_split:String)
  :List[(VertexId,VertexId,EdgeLabel)]={

    val h_conf = HBaseConfiguration.create()
    h_conf.set("hbase.zookeeper.quorum", "slave001,slave002,slave003")
    h_conf.set(TableOutputFormat.OUTPUT_TABLE, htable_name)
    val h_table = new HTable(h_conf, htable_name)
    /**
      * Query interval
      */
//        var res_all:List[(VertexId,VertexId,EdgeLabel)]=List()
//        for(i:Int<-0 until (res_edges_maynotin.length,Hbase_interval)){
//          val sublist=res_edges_maynotin.subList(i,Math.min(i+Hbase_interval,res_edges_maynotin.length))
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

  def updateHbase_origin(edge_processed:RDD[(VertexId,VertexId,EdgeLabel)],nodes_num_bitsize:Int,
                         symbol_num_bitsize:Int,h_conf:Configuration,h_job:Job,h_table:HTable,output:String,htable_split_Map:Map[Int,String],
                         htable_nodes_interval:Int,default_split:String)= {
    println("Update HBase Using BulkLoad")
    edge_processed.map(s => {
      val final_string = Edge2String(s,nodes_num_bitsize,symbol_num_bitsize,htable_split_Map, htable_nodes_interval,default_split)
      val final_rowkey = Bytes.toBytes(final_string)
      val kv: KeyValue = new KeyValue(final_rowkey, "edges".getBytes, "contents".getBytes, Bytes.toBytes('1'))
      (final_string, (new ImmutableBytesWritable(final_rowkey), kv))
    }).sortByKey().map(s => s._2)
      .saveAsNewAPIHadoopFile(output, classOf[ImmutableBytesWritable],
        classOf[KeyValue],
        classOf[HFileOutputFormat], h_job.getConfiguration())

    val bulkLoader = new LoadIncrementalHFiles(h_job.getConfiguration())
    bulkLoader.doBulkLoad(new Path(output), h_table)
  }

  def updateHbase(edge_processed:RDD[(VertexId,VertexId,EdgeLabel)],nodes_num_bitsize:Int,
                  symbol_num_bitsize:Int,htable_name:String,output:String,htable_split_Map:Map[Int,String],
                  htable_nodes_interval:Int,default_split:String)= {
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
      val final_string = Edge2String(s,nodes_num_bitsize,symbol_num_bitsize,htable_split_Map, htable_nodes_interval,default_split)
      val final_rowkey = Bytes.toBytes(final_string)
      val kv: KeyValue = new KeyValue(final_rowkey, "edges".getBytes, "contents".getBytes, Bytes.toBytes('1'))
      (final_string, (new ImmutableBytesWritable(final_rowkey), kv))
    }).sortByKey().map(s => s._2)
      .saveAsNewAPIHadoopFile(output, classOf[ImmutableBytesWritable],
        classOf[KeyValue],
        classOf[HFileOutputFormat], h_job.getConfiguration())

    val bulkLoader = new LoadIncrementalHFiles(h_job.getConfiguration())
    bulkLoader.doBulkLoad(new Path(output), h_table)
    h_table.close()
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
  //      p.add("edges".getBytes(), "contents".getBytes, Bytes.toBytes("1"))
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
  //          p.add("edges".getBytes(), "contents".getBytes, Bytes.toBytes("1"))
  //        })
  //        h_table.put(p_l)
  //      }
  //      h_table.close()
  //    })
  //
  //    println("End Update HBase")
  //  }
}
