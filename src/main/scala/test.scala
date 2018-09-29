/**
  * Created by cycy on 2018/1/26.
  */
import java.lang.Exception
import java.util.Scanner

import org.apache.commons.collections.functors.ExceptionFactory
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import utils.{BIgSpa_OP, BigSpa_OP_java, HBase_OP, deleteDir}

import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import scala.util.Random
import it.unimi.dsi.fastutil.ints._
import it.unimi.dsi.fastutil.longs.{LongArrayList, LongComparator, LongOpenHashSet}
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.util.Bytes
object test {
  def main(args: Array[String]): Unit = {
    val buffer=new ArrayBuffer[Int](4)
    buffer.append(1)
    println(buffer.size)
  }

  }

class Long_Comparator extends LongComparator {
  def compare(a1: Long, a2: Long): Int = if (a1 < a2) -1
  else if (a1 > a2) 1
  else 0
}