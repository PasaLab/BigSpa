package utils.Statistic

import java.io.{File, PrintWriter}
import java.util.Scanner

import scala.collection.mutable.ArrayBuffer
import scala.io.Source

/**
  * Created by cycy on 2018/4/20.
  */
object split_TaskTime {
  def split_Linux_Union(): Unit ={
    var input:String="H:\\Graspan资料\\实验记录\\论文数据\\Linux_PT_Redis"
    var output_map_time: String = input+"\\map_time" //除去ip地址

    var target_shuffle_read="Shuffle Read Time:"
    var target_shuffle_write="Shuffle Write Time:"
    var target_compute="Executor Computing Time:"

    var node_num=16
    val out_map_time=new PrintWriter(output_map_time)
    val iter_num=new File(input+"/Balance").listFiles()
    val scan=new Scanner(System.in)
    val res=new Array[Array[Int]](node_num)
    for(i<-0 to 15){
      res(i)=Array(i,0)
    }
    for(d<-iter_num){
      val path=d
      println(d.toString)
      println(d.toString.split("Stage")(1).split("(Attempt 0)")(0).dropRight(1))
      val fileindex=d.toString.split("Stage")(1).split("(Attempt 0)")(0).dropRight(1).trim.toInt
      val source=Source.fromFile(path).getLines().toArray
      var index=0
      var len=0
      var sum_time=0.0
      while(index<source.length){
        if(source(index).contains("<td>slave")){
          val node_index=source(index).split("slave")(1).substring(0,3).toInt
//          val time=source(index+1).split(">")(1).split("<")(0).dropRight(1).trim
          val time=source(index+1).split(">")(1).split("<")(0).trim
          println(node_index+":"+time)
          if(time.contains("ms")) res(node_index-2)(1)+= time.split("ms")(0).trim.toInt/1000
          else if (time.contains("s")) res(node_index-2)(1)+= time.split("s")(0).trim.toInt
          else res(node_index-2)(1)+= (time.split("min")(0).trim.toDouble*60).toInt
        }
        index+=1
      }
    }
   for(i<- res){
     out_map_time.println((i(0)+1)+"\t"+(i(1).toDouble/3600))
   }
    out_map_time.close()
  }
  def split_other_Union(): Unit ={
    var input:String="H:\\Graspan资料\\实验记录\\论文数据\\httpd_PT_Redis"
    var output_shuffle: String = input+"\\shuffle" //除去ip地址
    var output_compute: String = input+"\\compute" //除去ip地址

    var target_shuffle_read="Shuffle Read Time:"
    var target_shuffle_write="Shuffle Write Time:"
    var target_compute="Executor Computing Time:"

    val out_shuffle=new PrintWriter(output_shuffle)
    val out_compute=new PrintWriter(output_compute)
    val iter_num=new File(input+"/union").listFiles()
    val scan=new Scanner(System.in)
    val tmp_res=new ArrayBuffer[(Int,Double,Double)]()
    for(d<-iter_num){
      val path=d
      println(d)
      val fileindex=d.toString.split("Stage")(1).split("(Attempt 0)")(0).dropRight(1).trim.toInt
      val source=Source.fromFile(path).getLines()
      var continue=true
      while(source.hasNext&&continue){
        val strs=source.next()
        if(strs.contains("<br>")){
          continue=false
          val str_shuffle_read=strs.split("<br>").filter(_.contains(target_shuffle_read))
          val str_shuffle_write=strs.split("<br>").filter(_.contains(target_shuffle_write))
          val str_compute=strs.split("<br>").filter(_.contains(target_compute))
          val sum_shuffle_read=str_shuffle_read.map(s=>{
            val a=s.split(target_shuffle_read)(1)
            if(a.contains("ms")) a.trim.split("ms")(0).toDouble
            else a.trim.split("s")(0).toDouble*1000
          }).sum
          val sum_shuffle_write=str_shuffle_write.map(s=>{
            val a=s.split(target_shuffle_write)(1)
            if(a.contains("ms")) a.trim.split("ms")(0).toDouble
            else a.trim.split("s")(0).toDouble*1000
          }).sum
          val sum_compute=str_compute.map(s=>{
            val a=s.split(target_compute)(1)
            if(a.contains("ms")) a.trim.split("ms")(0).toDouble
            else a.trim.split("s")(0).toDouble*1000
          }).sum
          if(str_shuffle_read.length != str_shuffle_write.length||str_shuffle_write.length!=str_compute.length)
            println("!!!!!!!!!!!ERROR")
          val len=str_compute.length
          tmp_res.append((fileindex,(sum_shuffle_read+sum_shuffle_write)/len,sum_compute/len))
        }
      }
    }
    val res=tmp_res.sortWith((x,y)=>x._1<y._1)
    println(res.mkString("\n"))
    for(i<-res){
      out_shuffle.println(i._2)
      out_compute.println(i._3)
    }
    out_shuffle.close()
    out_compute.close()
  }
  def main(args: Array[String]): Unit = {
    var islocal: Boolean = true
    var master: String = "local"
    split_Linux_Union()
  }
}
