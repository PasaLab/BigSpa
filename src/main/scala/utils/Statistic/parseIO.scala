package utils.Statistic

/**
  * Created by cycy on 2018/4/27.
  */

import java.io.{File, PrintWriter}

import scala.collection.mutable.ArrayBuffer
import scala.io.Source
object parseIO {
  type hour=Int
  type min=Int
  type sec=Int
  type in=Double
  type out=Double
  /**
  CPU
    "time", "in", "out"
                 0  1  2  3 4 5   6     7
    [ new Date(2018,3,27,10,2,2), 0, -0.54064],
    */
  def getIO(path:String): Array[((hour,min,sec),(in,out))] ={
    val res_tmp=new ArrayBuffer[((hour,min,sec),(in,out))]()
    val allfiles=new File(path).listFiles()
    for(iter<-allfiles){
      val source=Source.fromFile(iter)
      val lineIterator=source.getLines()
      for(l<-lineIterator){
        println(l)
        if(l.split(",").length>7){
          val strs=l.split(",").map(s=>s.trim)
          res_tmp.append((
            (strs(3).toInt,
              strs(4).toInt,
              strs(5).dropRight(1).toInt),
            (strs(6).toDouble,
              strs(7).dropRight(1).toDouble * -1)
            ))
        }
      }
    }
    val res=res_tmp.distinct.sortWith((x,y)=>{
      if(x._1._1<y._1._1) true
      else if(x._1._1>y._1._1) false
      else{
        if(x._1._2<y._1._2) true
        else if(x._1._2>y._1._2) false
        else{
          if(x._1._3<y._1._3) true
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
    val input_log=input+"扩展性实验\\1.txt"
    val input_IO=input+"扩展性实验\\Netdata"
    val output_In=input+"in.txt"
    val output_Out=input+"out.txt"
    val turn_time=gettime(input_log)
    val each_IO=getIO(input_IO)
    //    println(turn_time.mkString("\n"))
    //    println(each_MEM.mkString("\n"))
    //    println(each_CPU.mkString("\n"))
    val turn_In=new ArrayBuffer[Array[Double]]()
    val turn_Out=new ArrayBuffer[Array[Double]]()
    var index=0
    for(time<-turn_time){
      val tmp_store_In=new ArrayBuffer[Double]()
      val tmp_store_Out=new ArrayBuffer[Double]()
      for(t<-0 until time){
        println(index+" "+each_IO.length)
        if(index<each_IO.length) tmp_store_In.append(each_IO(index)._2._1)
        if(index<each_IO.length) tmp_store_Out.append(each_IO(index)._2._2)
        index+=1
      }
      turn_In.append(tmp_store_In.toArray)
      turn_Out.append(tmp_store_Out.toArray)
    }

    val out_In=new PrintWriter(output_In)
    val out_Out=new PrintWriter(output_Out)

    val turn =turn_time.length
    for(i<-0 until turn){
      if(!turn_In(i).isEmpty){
        out_In.println(turn_In(i).sum/turn_In(i).length)
      }
      if(!turn_Out(i).isEmpty){
        out_Out.println(turn_Out(i).sum/turn_Out(i).length)
      }
    }
    out_In.close()
    out_Out.close()
  }
}
