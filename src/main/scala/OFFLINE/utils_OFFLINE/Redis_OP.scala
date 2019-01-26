package OFFLINE.utils_OFFLINE

import cn.edu.nju.pasalab.db.{BasicKVDatabaseClient, ShardedRedisClusterClient, Utils}
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer

/**
  * Created by cycy on 2018/4/13.
  */
class Redis_OP extends DataBase_OP with Serializable {

  var updateRedis_interval: Int=0
  def this(interval:Int){
    this()
    updateRedis_interval=interval
  }
  def Array2ByteArray(edge:Array[Int]):Array[Byte]={
    val res=new Array[Byte](9)
    res(0)=((edge(0) >> 24) & 0xFF).toByte
    res(1)=((edge(0) >> 16) & 0xFF).toByte
    res(2)=((edge(0) >> 8) & 0xFF).toByte
    res(3)=(edge(0) & 0xFF).toByte

    res(4)=((edge(1) >> 24) & 0xFF).toByte
    res(5)=((edge(1) >> 16) & 0xFF).toByte
    res(6)=((edge(1) >> 8) & 0xFF).toByte
    res(7)=(edge(1) & 0xFF).toByte

    res(8)=(edge(2) & 0xFF).toByte

    res
  }

  def Triple2String(src:Int,dst:Int,edgelabel:Int):Array[Byte]={
    val res=new Array[Byte](9)
    res(0)=((src >> 24) & 0xFF).toByte
    res(1)=((src >> 16) & 0xFF).toByte
    res(2)=((src >> 8) & 0xFF).toByte
    res(3)=(src & 0xFF).toByte

    res(4)=((dst >> 24) & 0xFF).toByte
    res(5)=((dst >> 16) & 0xFF).toByte
    res(6)=((dst >> 8) & 0xFF).toByte
    res(7)=(dst & 0xFF).toByte

    res(8)=(edgelabel & 0xFF).toByte

    res
  }

  /**
    * Update Redis
    * @param edges
    */
  @throws[Exception]
  def updateRedis_inPartition(edges: Iterator[Array[Int]]) {
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
          keys(i) = Array2ByteArray(items(i))
          values(i) =Array(1.toByte)
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

  def Update(edges: RDD[Array[Int]]) {
    edges.foreachPartition(e=>updateRedis_inPartition(e))
  }


  @throws[Exception]
  def Query_DF(compressed_edges: Array[Long]):
  Array[Array[Int]] = {
    val client: BasicKVDatabaseClient = ShardedRedisClusterClient.getProcessLevelClient
    val all_edges_num: Int = compressed_edges.length
    val keys: Array[Array[Byte]] = new Array[Array[Byte]](all_edges_num)
    val res:ArrayBuffer[Array[Int]]=new ArrayBuffer[Array[Int]](all_edges_num)
    var i: Int = 0
    while (i < all_edges_num) {
      keys(i) =Triple2String((compressed_edges(i) & 0xffffffffL).toInt,(compressed_edges(i)>>>32).toInt, 0)
      i += 1
    }
    val values: Array[Array[Byte]] = client.getAll(keys)
    i=0
    while (i<all_edges_num){
      if(values(i)==null) res.append(Array((compressed_edges(i) & 0xffffffffL).toInt,(compressed_edges(i)>>>32).toInt,0))
      i+=1
    }
    res.toArray
  }

  /**
    * Query PT
    * @param compressed_edges
    * @return
    */
  @throws[Exception]
  def Query_PT(compressed_edges: Array[Int]): Array[Array[Int]] = {
    val client: BasicKVDatabaseClient = ShardedRedisClusterClient.getProcessLevelClient
    val all_edges_num: Int = compressed_edges.length / 3
    val keys: Array[Array[Byte]] = new Array[Array[Byte]](all_edges_num)
    val res:ArrayBuffer[Array[Int]]=new ArrayBuffer[Array[Int]](all_edges_num)
    var i: Int = 0
    while (i < all_edges_num) {
      {
        keys(i) =Triple2String(compressed_edges(i * 3), compressed_edges(i * 3 + 1), compressed_edges(i * 3 + 2))
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

  /**
    * Query PT
    * @param compressed_edges
    * @return
    */
  @throws[Exception]
  def Query_PT_Split(compressed_edges: Array[Int],additinal_point:ArrayBuffer[(Int,Int,Int)],origin_nodes_num:Int):
  Array[Array[Int]]
  = {
    val client: BasicKVDatabaseClient = ShardedRedisClusterClient.getProcessLevelClient
    val all_edges_num: Int = compressed_edges.length / 3
    val keys: Array[Array[Byte]] = new Array[Array[Byte]](all_edges_num)
    val res:ArrayBuffer[Array[Int]]=new ArrayBuffer[Array[Int]](all_edges_num)
    var i: Int = 0
    while (i < all_edges_num) {
      {
        keys(i) =Triple2String(getRealVertex(compressed_edges(i * 3 ),additinal_point,origin_nodes_num),
          getRealVertex(compressed_edges(i * 3 + 1),additinal_point,origin_nodes_num),
          compressed_edges
        (i * 3 + 2))
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

  def getRealVertex(v:Int,additinal_point:ArrayBuffer[(Int,Int,Int)],origin_nodes_num:Int):Int={
    if(v<origin_nodes_num) v
    else{
      var res=v
      var f=0
      var b=additinal_point.size-1
      var continue=true
      while(f<=b&&continue){
        val mid=f+(b-f)/2
        if(additinal_point(mid)._1<=v&&additinal_point(mid)._2>=v){
          res=additinal_point(mid)._3
          continue=false
        }
        else if(additinal_point(mid)._1>v) b=mid-1
        else f=mid+1
      }
      if(continue) f
      else res
    }
  }

}
