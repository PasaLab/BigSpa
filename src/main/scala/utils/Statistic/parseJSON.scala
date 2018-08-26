package utils.Statistic

/**
  * Created by cycy on 2018/4/18.
  */
import java.io.PrintWriter

import scala.collection.mutable.ArrayBuffer
import scala.io.Source
object parseJSON {
  type hour=Int
  type min=Int
  type sec=Int
  type cpu_usage=Double
  type mem=Double
  /**
    CPU
    "time", "libstoragemgmt", "experiment", "root", "pcp", "unbound"
                 0  1  2  3  4  5   6     7       8      9   10
    [ new Date(2018,3,18,20,14,13), 0, 1.52183, 3.50031, 0, null],
		[ new Date(2018,3,18,20,14,12), 0, 3.56168, 3.46224, 0, null],
    */
  def getCPU(path:String): Array[(hour,min,sec,cpu_usage)] ={
    val source=Source.fromFile(path)
    val lineIterator=source.getLines()
    val res_tmp=new ArrayBuffer[(hour,min,sec,cpu_usage)]()
    for(l<-lineIterator){
      val strs=l.split(",").map(s=>s.trim)
//      println(strs(5))
      res_tmp.append((
        strs(3).toInt,
        strs(4).toInt,
        strs(5).dropRight(1).toInt,
        strs(8).toDouble/24
        ))
    }
    val res=res_tmp.sortWith((x,y)=>{
      if(x._1<y._1) true
      else if(x._1>y._1) false
      else{
        if(x._2<y._2) true
        else if(x._2>y._2) false
        else{
          if(x._3<y._3) true
          else false
        }
      }
    }).toArray
//    for(l<-res) println(l._1 + ":"+l._2+":"+l._3+" "+l._4)
    res
  }

  /**
    *  MEM
    "time", "libstoragemgmt", "experiment", "root", "pcp", "unbound"
                 0  1  2  3  4 5      6       7
    [ new Date(2018,3,18,13,34,7), 0.03125, 9186.957, 18.76172, 0, null],
		[ new Date(2018,3,18,13,34,6), 0.03125, 9186.693, 18.76172, 0, null],
    */
  def getMEM(path:String): Array[(hour,min,sec,cpu_usage)] ={
    val source=Source.fromFile(path)
    val lineIterator=source.getLines()
    val res_tmp=new ArrayBuffer[(hour,min,sec,cpu_usage)]()
    for(l<-lineIterator){
      val strs=l.split(",").map(s=>s.trim)
      res_tmp.append((
        strs(3).toInt,
        strs(4).toInt,
        strs(5).dropRight(1).toInt,
        strs(8).toDouble
        ))
    }
    val res=res_tmp.sortWith((x,y)=>{
      if(x._1<y._1) true
      else if(x._1>y._1) false
      else{
        if(x._2<y._2) true
        else if(x._2>y._2) false
        else{
          if(x._3<y._3) true
          else false
        }
      }
    }).toArray
//    for(l<-res) println(l._1 + ":"+l._2+":"+l._3+" "+l._4)
    res
  }
  def gettime(input:String): Array[Int] ={
    val source=Source.fromFile(input).getLines()
    val res=new ArrayBuffer[Int]()
    for(l<-source){
      if(l.contains("*step:")){
        val tmp=l.split(" ")
        res.append((tmp(tmp.length-2).toDouble+0.5).toInt)
      }
    }
    res.toArray
  }
  def main(args: Array[String]): Unit = {
    val input="H:\\Graspan资料\\实验记录\\论文数据\\Linux_PT_Redis\\"
    val input_log=input+"log.txt"
    val input_CPU=input+"CPU.txt"
    val input_MEM=input+"MEM.txt"
    val output_MEM_ave=input+"MEM_ave"
    val output_MEM_max=input+"MEM_max"
    val output_CPU_ave=input+"CPU_ave"
    val output_CPU_max=input+"CPU_max"
    val turn_time=gettime(input_log)
    val each_MEM=getMEM(input_MEM)
    val each_CPU=getCPU(input_CPU)
//    println(turn_time.mkString("\n"))
//    println(each_MEM.mkString("\n"))
//    println(each_CPU.mkString("\n"))
    val turn_MEM=new ArrayBuffer[Array[Double]]()
    val turn_CPU=new ArrayBuffer[Array[Double]]()
    var index=0
    for(time<-turn_time){
      val tmp_store_MEM=new ArrayBuffer[Double]()
      val tmp_store_CPU=new ArrayBuffer[Double]()
      for(t<-0 until time){
        println(index+" "+each_MEM.length+" "+each_CPU.length)
        if(index<each_MEM.length) tmp_store_MEM.append(each_MEM(index)._4)
        if(index<each_CPU.length) tmp_store_CPU.append(each_CPU(index)._4)
        index+=1
      }
      turn_MEM.append(tmp_store_MEM.toArray)
      turn_CPU.append(tmp_store_CPU.toArray)
    }

    val out_MEM_ave=new PrintWriter(output_MEM_ave)
    val out_MEM_max=new PrintWriter(output_MEM_max)
    val out_CPU_ave=new PrintWriter(output_CPU_ave)
    val out_CPU_max=new PrintWriter(output_CPU_max)

    val turn =turn_time.length
    for(i<-0 until turn){
      if(!turn_MEM(i).isEmpty){
        out_MEM_ave.println(turn_MEM(i).sum/turn_MEM(i).length)
        out_MEM_max.println(turn_MEM(i).max)
      }
      if(!turn_CPU(i).isEmpty){
        out_CPU_ave.println(turn_CPU(i).sum/turn_CPU(i).length)
        out_CPU_max.println(turn_CPU(i).max)
      }
    }
    out_MEM_ave.close()
    out_MEM_max.close()
    out_CPU_ave.close()
    out_CPU_max.close()
  }
}
