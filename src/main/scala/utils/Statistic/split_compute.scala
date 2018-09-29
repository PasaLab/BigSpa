package utils.Statistic

/**
  * Created by cycy on 2018/4/20.
  */
import java.io.{PrintWriter, _}

import org.apache.spark.SparkContext

import scala.io.Source
object split_compute {
  def selectLine(sc:SparkContext,input:String,targetline:String,output:String): Unit ={
    val data=sc.textFile(input).zipWithIndex().map(s=>s.swap).sortByKey().filter(s=> !s._2.trim.equals("")&&s._2.contains
    (targetline)).map(s=>{
      val strs=s._2.split("\\s+")
      if(strs.last.contains("sec"))
        strs(strs.length-2)
      else strs.last
    }).zipWithIndex().map(s=>s.swap).sortByKey().map(s=>s._2).repartition(1)
      .saveAsTextFile(output)
  }
  def removestar(sc:SparkContext,input:String,output:String)={
    val data=sc.textFile(input).zipWithIndex().map(s=>s.swap).sortByKey().filter(s=> !s._2.contains("*")).map(s=>{
      val strs=s._2.split("\\s+")
      if(strs.last.contains("sec"))
        strs(strs.length-2)
      else strs.last
    }).zipWithIndex().map(s=>s.swap).sortByKey().map(s=>s._2).repartition(1)
      .saveAsTextFile(output)
  }
  def sample(sc:SparkContext,input:String,internal:Int,output:String): Unit ={
    val data=sc.textFile(input).filter(s=> !s.trim.equals("")).zipWithIndex().filter(s=>s._2<=1000000).map(s=>s
      ._1).repartition(1).saveAsTextFile(output)
  }

  def main(args: Array[String]): Unit = {
    var islocal: Boolean = true
    var master: String = "local"

    var input:String="H:\\Graspan资料\\实验记录\\论文数据\\PSQL_PT_Redis"
    var output_join: String = input+"\\join" //除去ip地址
    var output_redis: String = input+"\\redis" //除去ip地址
    val out_join=new PrintWriter(output_join)
    val out_redis=new PrintWriter(output_redis)
    val iter_num=new File(input+"/par_INFO").listFiles().length
    for(d<-1 to iter_num){
      val path=input+"/par_INFO/step"+d+"/part-00000"
      val source=Source.fromFile(path)
      val lines=source.getLines()
      var sum_join=0.0
      var sum_redis=0.0
      var sum_par=0
      for(l<-lines){
        if(!l.trim.equals("")) {
          sum_par += 1
          sum_join += (l.split("REPARJOIN")) (1).trim.toDouble
          sum_redis += l.split("REPARREDIS")(1).trim.toDouble
        }
      }
      println("par: "+sum_par+" sum_join/16: "+sum_join/16+" sum_redis/16: "+sum_redis/16)
      out_join.println(sum_join/sum_par)
      out_redis.println(sum_redis/sum_par)
    }
    out_join.close()
    out_redis.close()
  }
}

