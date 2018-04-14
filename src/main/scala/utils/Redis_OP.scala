package utils

import cn.edu.nju.pasalab.db.{BasicKVDatabaseClient, ShardedRedisClusterClient, Utils}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by cycy on 2018/4/13.
  */
object Redis_OP {

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
  def Array2ByteArray(edge:Array[Int],nodes_num_bitsize:Int,symbol_num_bitsize:Int):Array[Byte]={
    val src_str = filling0(edge(0), nodes_num_bitsize)
    val dst_str = filling0(edge(1), nodes_num_bitsize)
    val label_str = filling0(edge(2), symbol_num_bitsize)
    (src_str + dst_str + label_str).getBytes()
    //    src_str + dst_str + label_str
  }

  def Triple2String(src:Int,dst:Int,edgelabel:Int,nodes_num_bitsize:Int,symbol_num_bitsize:Int):Array[Byte]={
    val src_str = filling0(src, nodes_num_bitsize)
    val dst_str = filling0(dst, nodes_num_bitsize)
    val label_str = filling0(edgelabel, symbol_num_bitsize)
    (src_str + dst_str + label_str).getBytes()
    //    src_str + dst_str + label_str
  }

  @throws[Exception]
  def updateRedis_inPartition(edges: Iterator[Array[Int]], updateRedis_interval: Int,nodes_num_bitsize:Int,symbol_num_bitsize:Int) {
    val items = edges.toArray
    try{
      val client: BasicKVDatabaseClient = ShardedRedisClusterClient.getProcessLevelClient
      var keys: Array[Array[Byte]] = null
      var values: Array[Array[Byte]] = null
      keys = new Array[Array[Byte]](items.size)
      values = new Array[Array[Byte]](items.size)
      var i: Int = 0
      while (i < items.size) {
        {
          keys(i) = Array2ByteArray(items(i),nodes_num_bitsize,symbol_num_bitsize)
          values(i) ="1".getBytes()
        }
        {
          i += 1
        }
      }
      Utils.batchInput(client, keys, values, updateRedis_interval)
  }
    catch {
      case e: Exception => {
        e.printStackTrace()
      }
    }
  }

  @throws[Exception]
  def queryRedis_compressed(compressed_edges: Array[Int],nodes_num_bitsize:Int,symbol_num_bitsize:Int): Array[Array[Int]] = {
    val client: BasicKVDatabaseClient = ShardedRedisClusterClient.getProcessLevelClient
    val all_edges_num: Int = compressed_edges.length / 3
    val keys: Array[Array[Byte]] = new Array[Array[Byte]](all_edges_num)
    val res:ArrayBuffer[Array[Int]]=new ArrayBuffer[Array[Int]](all_edges_num)
    var i: Int = 0
    while (i < all_edges_num) {
      {
        keys(i) =Triple2String(compressed_edges(i * 3), compressed_edges(i * 3 + 1), compressed_edges(i * 3 + 2),
          nodes_num_bitsize,symbol_num_bitsize)
      }
      {
        i += 1
      }
    }
    val values: Array[Array[Byte]] = client.getAll(keys)
    i=0
    while (i<all_edges_num){
      if(values(i)==null) res.append(Array(compressed_edges(i*3),compressed_edges(i*3+1),compressed_edges(i*3+2)))
      i+=1
    }
    res.toArray
  }

  def queryRedis(edges: Array[Array[Int]],nodes_bitnum:Int,symbol_bitnum:Int): Array[Array[Int]] = {
    val client: BasicKVDatabaseClient = ShardedRedisClusterClient.getProcessLevelClient
    val all_edges_num: Int = edges.length
    val keys: Array[Array[Byte]] = new Array[Array[Byte]](all_edges_num)
    var i: Int = 0
    while (i < all_edges_num) {
      {
        keys(i) = Array2ByteArray(edges(i),nodes_bitnum,symbol_bitnum)
      }
      {
        i += 1
      }
    }
    val values: Array[Array[Byte]] = client.getAll(keys)
    (edges zip values).filter(s=>s._2==null).map(s=>s._1)
  }
}
