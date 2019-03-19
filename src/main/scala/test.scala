/**
  * Created by cycy on 2018/1/26.
  */
import java.io.PrintWriter
import java.{lang, util}
import java.util.Scanner

import ONLINE.ProtocolBuffer.ProtocolBuffer_OP
import ONLINE.ProtocolBuffer.ProtocolBuffer_OP._
//import ONLINE.Query_Filter_Compute_Update
//import ONLINE.Query_Filter_Compute_Update._
import ONLINE.utils_ONLINE.Redis_OP

import scala.collection.mutable.ArrayBuffer

import it.unimi.dsi.fastutil.longs.{LongArrayList, LongComparator, LongOpenHashSet}

object test {
  def sleep(duration: Long) { Thread.sleep(duration) }
  def main(args: Array[String]): Unit = {
//    println("test map protocol buffer at driver")
//    val map:java.util.Map[Integer,java.lang.Long]=new util.HashMap[Integer,java.lang.Long]()
//    map.put(1,2l)
//    map.put(2,4l)
//    val str=Serialzed_Map_UidCounts(map)
//    println(new String(str))
//    val a=Array[Int]()
//    a.foreach(println)
//    println(Query_Filter_Compute_Update.DecodeVid_label_pos(3788066665791503l))
//    println(Query_Filter_Compute_Update.DecodeCounts(4294967296l))
//    val array=Array(1,2,3,4,5,6,7,8,9)
//    println(array.flatMap(s=>Array(s,s+10)).mkString(","))
//    val len=50000
//    val t0=System.nanoTime()
//    val array=new LongOpenHashSet()
//    for(i<-0 to len-1)
//      array.add(i.toLong)
//    println(array.size())
//    val t1=System.nanoTime()
//    println(s"distinct $len uses "+(t1-t0)/1e9)
  val bytes=new Array[Array[Byte]](2)
    bytes(0)=Array[Byte](3,3,3)
    bytes(1)=Array[Byte](1)
    println(new String(bytes(0))+" "+new String(bytes(1)))
  }


}

