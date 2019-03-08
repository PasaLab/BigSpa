/**
  * Created by cycy on 2018/1/26.
  */
import java.lang.Exception
import java.util
import java.util.Scanner

import ONLINE.ProtocolBuffer.ProtocolBuffer_OP
import ONLINE.Query_Filter_Compute_Update
import ONLINE.utils_ONLINE._
import ONLINE.utils_ONLINE.Redis_OP
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext, sql}
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import scala.util.Random
import it.unimi.dsi.fastutil.ints._
import it.unimi.dsi.fastutil.longs.{LongArrayList, LongComparator, LongOpenHashSet}
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.StructType
import org.apache.spark.streaming.{Seconds, StreamingContext}

object test {
  def main(args: Array[String]): Unit = {
    val map=new util.HashMap[Integer,java.lang.Long]()
    map.put(1,2l)
    map.put(2,4l)
    val keys=ProtocolBuffer_OP.getmapKeys(map)
    for(k<-keys) println(k)
  }
}

