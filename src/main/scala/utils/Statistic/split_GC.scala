package utils.Statistic

/**
  * Created by cycy on 2018/4/27.
  */

import java.io.{File, PrintWriter}
import java.util.Scanner

import scala.collection.mutable.ArrayBuffer
import scala.io.Source

object split_GC {
  def split_Linux_Union(): Unit ={
    var input:String="H:\\Graspan资料\\实验记录\\论文数据\\Linux_PT_Redis\\扩展性实验\\单节点实验情况\\GC\\map+distinct"
    var output_Duration: String = input+"\\duration.txt" //除去ip地址
    var output_GC: String = input+"\\GC.txt" //除去ip地址

    var par=3456
    val iter_num=new File(input).listFiles()
    val out_Duration=new PrintWriter(output_Duration)
    val out_GC=new PrintWriter(output_GC)
    val scan=new Scanner(System.in)
    val tmp_res=new ArrayBuffer[(Int,Double,Double)]()
    for(d<-iter_num){
      val path=d
//      println(d.toString)
//      println(d.toString.split("Stage")(1).split("(Attempt 0)")(0).dropRight(1))
      val fileindex=d.toString.split("Stage")(1).split("(Attempt 0)")(0).dropRight(1).trim.toInt
      val source=Source.fromFile(path).getLines().toArray
      var index=0
      var len=0
      var sum_duration=0.0
      var sum_GC=0.0
      var assert_num_GC=0
      var assert_num_duration=0
      while(index<source.length){
        if(source(index).contains("<td>2018/04/26")||source(index).contains("<td>2018/04/27")){
          val tmp=source(index+1).split(">")(1).split("<")(0).trim
          println("duration: "+tmp)
          if(tmp.contains("ms")) sum_duration+=tmp.split("ms")(0).toDouble/1000
          else if (tmp.contains("s"))sum_duration+=tmp.split("s")(0).toDouble
          else sum_duration+=tmp.split("mi")(0).toDouble*60
          assert_num_duration+=1
        }
        else if(source(index).contains("td class=\"deserialization_time\"")){
          val tmp=source(index+4).trim
          println("GC:"+tmp)
          if(tmp.contains("ms")) sum_GC+=tmp.split("ms")(0).toDouble/1000
          else if (tmp.contains("s"))sum_GC+=tmp.split("s")(0).toDouble
          else sum_GC+=tmp.split("mi")(0).toDouble*60
          assert_num_GC+=1
        }
        index+=1
      }
      tmp_res.append((fileindex,sum_duration/par,sum_GC/par))
      println(fileindex+": "+assert_num_duration+" "+assert_num_GC)
      if(assert_num_duration != assert_num_GC ) {
        println(fileindex+": "+assert_num_duration+" "+assert_num_GC)
        println("!!!!!ERROR")
      }
//      val scan=new Scanner(System.in)
//      scan.next()
    }
    val res=tmp_res.sortWith((x,y)=>x._1<y._1)
    println(res.mkString("\n"))
    for(i<-res){
      out_Duration.println(i._2)
      out_GC.println(i._3)
    }
    out_Duration.close()
    out_GC.close()
  }
  def main(args: Array[String]): Unit = {
    var islocal: Boolean = true
    var master: String = "local"
    split_Linux_Union()
  }
}
