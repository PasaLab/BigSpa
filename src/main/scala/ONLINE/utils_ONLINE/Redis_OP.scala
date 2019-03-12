package ONLINE.utils_ONLINE

import ONLINE.ProtocolBuffer.ProtocolBuffer_OP
import cn.edu.nju.pasalab.db.{BasicKVDatabaseClient, ShardedRedisClusterClient, Utils}
import org.apache.spark.rdd.RDD
import ONLINE.ProtocolBuffer.ProtocolBuffer_OP._

import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConversions.mapAsJavaMap
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

  def Long2ByteArray(l:Long):Array[Byte]={
    val res=new Array[Byte](8)
    for(i<-0 to 7)
      res(i)=((l>>(8*(7-i)))&0xff).toByte
    res
  }
  /**
    * Update Redis
    * @param edges
    */
  @throws[Exception]
  def updateRedis_inPartition0(edges: Iterator[Array[Int]]) {
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

  def Update0(edges: RDD[Array[Int]]) {
    edges.foreachPartition(e=>updateRedis_inPartition0(e))
  }

  /**
    * Update Redis with form (src,dst,label)
    * @param edges
    */
  def updateRedis_inPartition(edges: Iterator[(Int,Int,Int)]) {
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
          keys(i) = Triple2String(items(i)._1,items(i)._2,items(i)._3)
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

  def Update(edges: RDD[(Int,Int,Int)]) {
    edges.foreachPartition(e=>updateRedis_inPartition(e))
  }

  /**
    * Remove Redis
    */
  def removeRedis_inPartition(edges: Iterator[(Int,Int,Int)]) {
    val items = edges.toArray
    try{
      val client: BasicKVDatabaseClient = ShardedRedisClusterClient.getProcessLevelClient
      var keys: Array[Array[Byte]] = null
      keys = new Array[Array[Byte]](items.size)
      var i: Int = 0
      while (i < items.size) {
        {
          keys(i) = Triple2String(items(i)._1,items(i)._2,items(i)._3)
        }
        {
          i += 1
        }
      }
      Utils.batchRemove(client, keys, updateRedis_interval)
    }
    catch {
      case e: Exception => {
        e.printStackTrace()
      }
    }
  }
  def Remove(edges: RDD[(Int,Int,Int)]) {
    edges.foreachPartition(e=>removeRedis_inPartition(e))
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

  def Query_PT(add_edges:RDD[(Int,Int,Int)]):RDD[(Int,Int,Int)]=add_edges.mapPartitions(p=>Query_PT_inPartition
  (p))
  /**
    * 新边各分区查询Redis,判断是否为未出现的新边
    * @return
    */
  def Query_PT_inArray(edges: Array[(Int,Int,Int)]):ArrayBuffer[(Int,Int,Int)]={
    val client: BasicKVDatabaseClient = ShardedRedisClusterClient.getProcessLevelClient
    val keys: Array[Array[Byte]] = edges.map(e=>Triple2String(e._1,e._2,e._3))
    val res:ArrayBuffer[(Int,Int,Int)]=new ArrayBuffer[(Int,Int,Int)](edges.length)
    val values: Array[Array[Byte]] = client.getAll(keys)
    var i=0
    val edges_length=edges.length
    while (i<edges_length){
      if(values(i)==null) res.append(edges(i))
      i+=1
    }
    res
  }
  def Query_PT_inPartition(edges_iter: Iterator[(Int,Int,Int)]): Iterator[(Int,Int,Int)] = {
    val client: BasicKVDatabaseClient = ShardedRedisClusterClient.getProcessLevelClient
    val edges: Array[(Int,Int,Int)] = edges_iter.toArray
    val keys: Array[Array[Byte]] = edges.map(e=>Triple2String(e._1,e._2,e._3))
    val res:ArrayBuffer[(Int,Int,Int)]=new ArrayBuffer[(Int,Int,Int)](edges.length)
    val values: Array[Array[Byte]] = client.getAll(keys)
    var i=0
    val edges_length=edges.length
    while (i<edges_length){
      if(values(i)==null) res.append(edges(i))
      i+=1
    }
    res.toIterator
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


  /**
    * Spark Streaming + Redis hold all 版各种操作
    */

  def Update_PB(edges:RDD[(Long,java.util.Map[Integer,java.lang.Long])]): Unit ={
    edges.foreachPartition(p=>Update_PB_inPartition(p))
  }
  def Update_PB_inPartition(p:Iterator[(Long,java.util.Map[Integer,java.lang.Long])]): Unit ={
    val items = p.toArray
    println("items.length"+items.length)
        try{
          val client: BasicKVDatabaseClient = ShardedRedisClusterClient.getProcessLevelClient
          var keys: Array[Array[Byte]] = null
          var values: Array[Array[Byte]] = null
          keys = new Array[Array[Byte]](items.size)
          values = new Array[Array[Byte]](items.size)
          var i: Int = 0
          while (i < items.size) {
            {
              keys(i) = Long2ByteArray(items(i)._1)
              values(i) =Serialzed_Map_UidCounts(items(i)._2)
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

  // 以下两版 Array 更新，区别在于转换为Byte数组的操作放在何处，直接以Object 的Array数组进行Update，转换操作放在Redis， ByteArray则放在调用方完成
  def Update_PB_Array(items:Array[(Long,java.util.Map[Integer,java.lang.Long])]): Unit ={
    println("items.length"+items.length)
    try{
      val client: BasicKVDatabaseClient = ShardedRedisClusterClient.getProcessLevelClient
      var keys : Array[Array[Byte]]= new Array[Array[Byte]](items.size)
      var values : Array[Array[Byte]]= new Array[Array[Byte]](items.size)
      var i: Int = 0
      while (i < items.size) {
        {
          keys(i) = Long2ByteArray(items(i)._1)
          values(i) =Serialzed_Map_UidCounts(items(i)._2)
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

  def Update_PB_ByteArray(keys:Array[Array[Byte]],values:Array[Array[Byte]]): Unit ={
    assert(keys.length==values.length)
    val len=keys.length
    println("items.length"+keys.length)
    try{
      val client: BasicKVDatabaseClient = ShardedRedisClusterClient.getProcessLevelClient
      Utils.batchInput(client, keys, values, updateRedis_interval)
    }
    catch {
      case e: Exception => {
        e.printStackTrace()
      }
    }
  }

  def Query_PB_Array(Query:Array[Long]):Map[Long,java.util.Map[Integer,java.lang.Long]]={
    val client: BasicKVDatabaseClient = ShardedRedisClusterClient.getProcessLevelClient
    val keys: Array[Array[Byte]] = Query.map(q=>Long2ByteArray(q))
    val values: Array[Array[Byte]] = client.getAll(keys)
    val Answers:Array[java.util.Map[Integer,java.lang.Long]]=values.map(v=>ProtocolBuffer_OP.Deserialized_Map_UidCounts(v))
    (Query zip Answers).toMap
  }

  //这一步可能会造成大量冗余的map，但也可以一次性完成Query，减少以后的操作
  def QueryAndUpdate_PB_RDD_Partition(p_ecuc:Iterator[(((Int,Int,Int),Int),(Long,Long),Array[ArrayBuffer[(Long,Int)]])
    ]):Iterator[(((Int,Int,Int),Int),(Long,Long),Array[ArrayBuffer[(Long,Int)]],(java.util.Map[Integer,java.lang
  .Long],java.util.Map[Integer,java.lang.Long]),Array[Array[java.util.Map[Integer,java.lang.Long]]])]={
    val client: BasicKVDatabaseClient = ShardedRedisClusterClient.getProcessLevelClient
    val ecucs:Array[(((Int,Int,Int),Int),(Long,Long),Array[ArrayBuffer[(Long,Int)]])]=p_ecuc.toArray
    ecucs.map(ecuc=>{
      assert(ecuc._3.length==2)
      val (src_label_b,dst_label_f)=ecuc._2
      val (src_label_b_map,dst_label_f_map)=(client.get(Long2ByteArray(src_label_b)),client.get(Long2ByteArray(dst_label_f)))
      val compute_map:Array[Array[java.util.Map[Integer,java.lang.Long]]]=new Array(2)
      val compute_keys=ecuc._3
      println("compute_keys.length: "+compute_keys(0).length+" "+compute_keys(1).length)
      for(i<-0 to 1){
        compute_map(i)=new Array[java.util.Map[Integer,java.lang.Long]](compute_keys(i).length)
        val compute_key=compute_keys(i)
        for(j<-0 to compute_key.length-1){
          compute_map(i)(j)=ProtocolBuffer_OP.Deserialized_Map_UidCounts(client.get(Long2ByteArray(compute_key(j)._1)))
          if(compute_map(i)(j)!=null) println(s"compute_map($i)($j): "+null)
          else println(s"compute_map($i)($j).size: "+compute_map(i)(j).size())
        }
      }
      (ecuc._1,ecuc._2,ecuc._3,(ProtocolBuffer_OP.Deserialized_Map_UidCounts(src_label_b_map),ProtocolBuffer_OP
        .Deserialized_Map_UidCounts(dst_label_f_map)),
        compute_map)
    }).toIterator
  }
}
