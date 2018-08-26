package utils.Statistic

import java.io.{File, PrintWriter}
import java.util.Scanner

import scala.collection.mutable.ArrayBuffer
import scala.io.Source

/**
  * Created by cycy on 2018/4/20.
  */
object split_shuffle_compute {
  def split_Linux_Union(): Unit ={
    var input:String="H:\\Graspan资料\\实验记录\\论文数据\\Linux_PT_Redis"
    var output_shuffle: String = input+"\\shuffle" //除去ip地址
    var output_compute: String = input+"\\compute" //除去ip地址

    var target_shuffle_read="Shuffle Read Time:"
    var target_shuffle_write="Shuffle Write Time:"
    var target_compute="Executor Computing Time:"

    var par=2304
    val out_shuffle=new PrintWriter(output_shuffle)
    val out_compute=new PrintWriter(output_compute)
    val iter_num=new File(input+"/union").listFiles()
    val scan=new Scanner(System.in)
    val tmp_res=new ArrayBuffer[(Int,Double,Double)]()
    for(d<-iter_num){
      val path=d
      println(d.toString)
      println(d.toString.split("Stage")(1).split("(Attempt 0)")(0).dropRight(1))
      val fileindex=d.toString.split("Stage")(1).split("(Attempt 0)")(0).dropRight(1).trim.toInt
      val source=Source.fromFile(path).getLines().toArray
      var index=0
      var len=0
      var sum_duration=0.0
      var sum_shuffle=0.0
      var sum_other=0.0
      var assert_num_other=0
      var assert_num_duration=0
      var assert_num_shuffle=0
      while(index<source.length){
        if(source(index).contains("<td>2018/04/18")){
          //          println(source(index+1))
          val tmp=source(index+1).split(">")(1).split("<")(0).dropRight(1).trim
          if(tmp.contains("ms")) sum_duration+=tmp.split("ms")(0).toDouble
          else sum_duration+=tmp.split("s")(0).toDouble*1000
          assert_num_duration+=1
        }
        else if(source(index).contains("td class=\"scheduler_delay\"")
          || source(index).contains("td class=\"deserialization_time\"")
          ||source(index).contains("td class=\"serialization_time\"")
          ||source(index).contains("td class=\"getting_result_time\"")){
          //          println(source(index+1))
          val tmp=source(index+1).trim
          if(tmp.contains("ms")) sum_other+=tmp.split("ms")(0).toDouble
          else sum_other+=tmp.split("s")(0).toDouble*1000
          assert_num_other+=1
        }
        else if(source(index).contains("td class=\"fetch_wait_time\"")){
          //          println(source(index+1))
          val tmp=source(index+1).trim
          if(tmp.contains("ms")) sum_shuffle+=tmp.split("ms")(0).toDouble
          else sum_shuffle+=tmp.split("s")(0).toDouble*1000
          assert_num_shuffle+=1
        }
        index+=1
      }
      tmp_res.append((fileindex,sum_shuffle/par,(sum_duration-sum_other-sum_shuffle)/par))
      if(assert_num_duration != assert_num_shuffle || assert_num_duration*4!=assert_num_other) {
        println(fileindex+": "+assert_num_duration+" "+assert_num_shuffle+" "+assert_num_other)
        println("!!!!!ERROR")
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
