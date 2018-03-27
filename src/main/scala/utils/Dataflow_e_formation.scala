package utils

import java.io.{BufferedReader, FileReader}
import java.net.URI
import java.util.Scanner

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by cycy on 2018/3/26.
  */

object Dataflow_e_formation {
  val master:String="hdfs://slave001:9001/"
  val input_e_nomaster="/jiangxi/Graspan/data/InputGraph/Apache_Httpd_2.2.18_Dataflow/e/part-00000"
  var e_edges:Array[(Int,Array[Int])]=null
  def get_e(master:String,input_e_nomaster:String,parId:Int):Array[(Int,Array[Int])]={
    println("Partition "+parId+" Enter!!!")
    if(e_edges==null){
      this.synchronized{
        if(e_edges==null){
          println("Partition "+parId+" form e!!!")
          val e: ArrayBuffer[(Int, Int)] = ArrayBuffer()
          val fileSystem = FileSystem.get(new URI(master), new Configuration())
          // 调用open方法进行下载，参数HDFS路径
          val in = fileSystem.open(new Path(input_e_nomaster))
          val scanner = new Scanner(in)
          var tmp: String = null
          while (scanner.hasNextLine) {
            tmp=scanner.nextLine()
//            println("tmp: "+tmp)
            if (!tmp.trim.equals("")) {
//                println("valid tmp: "+tmp)
                val strs = tmp.split("\\s+")
//                println(strs.mkString(","))
                e.append((strs(0).toInt, strs(1).toInt))
              }
            }
//          val scan=new Scanner(System.in)
//          scan.next()
          e_edges=e.groupBy(_._1).toArray.sortBy(_._1).map(s => (s._1, s._2.map(x => x._2).toArray))
          in.close()
          println("end form e, e length "+e_edges.length)
        }
      }
    }
    else println("Partition "+parId+" already found e")
    e_edges
  }
}
