package utils.Statistic

/**
  * Created by cycy on 2018/1/17.
  */
import org.apache.spark.{SparkConf, SparkContext}
import utils.{Para, deleteDir}
object Statistic extends Para{
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

    var input_graph: String = "H:/Graspan资料/Graspan数据和源代码/Apache_Httpd_2.2.18_Points-to/Apache_httpd_2.2.18_pointsto_graph"
    var input_grammar: String = "H:/Graspan资料/Graspan数据和源代码/Grammar_Files/rules_pointsto"

    var input:String="H:\\Graspan资料\\实验记录\\论文数据\\Linux_PT_Redis\\log.txt"
    var output: String = "data/result/" //除去ip地址

    var par: Int = 4

    val conf = new SparkConf()//.set("spark.kryoserializer.buffer.max", "128")
    if (islocal) {
      //test location can be adjusted or not
      conf.setAppName("Graspan")
      System.setProperty("hadoop.home.dir", "F:/hadoop-2.6.0/")
      conf.setMaster("local")
    }

    var start=0
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    deleteDir.deletedir(islocal,master,output)

//    val tmp_graph=sc.textFile(input).filter(!_.trim.equals("")).map(s=>{
//      val str=s.split("\\s+")
//      (str(0).toInt,str(1).toInt,str(2))
//    })
//    val nodes_order=tmp_graph.flatMap(s=>List(s._1,s._2)).distinct()
//    val rdd=nodes_order
////    val rdd=sc.textFile(input).map(s=>s.split("\\s+")).flatMap(s=>List(s(0).toInt,s(1).toInt))
//    println("all nodes : "+nodes_order.count)
//    println("min : "+rdd.min())
//    println("max : "+rdd.max())
//    sample(sc,input_graph,0,output)

    var targetLine:String="coarest num"
    deleteDir.deletedir(islocal,master,output+targetLine)
    selectLine(sc,input,targetLine,output+targetLine)

//    targetLine="pure_newedges"
//    deleteDir.deletedir(islocal,master,output+targetLine)
//    selectLine(sc,input,targetLine,output+targetLine)
//
//    targetLine="distinct take time"
//    deleteDir.deletedir(islocal,master,output+targetLine)
//    selectLine(sc,input,targetLine,output+targetLine)
//
//    targetLine="compute take time"
//    deleteDir.deletedir(islocal,master,output+targetLine)
//    selectLine(sc,input,targetLine,output+targetLine)
//
//    targetLine="update Redis take time"
//    deleteDir.deletedir(islocal,master,output+targetLine)
//    selectLine(sc,input,targetLine,output+targetLine)
//
////    targetLine="n_edges time take time"
//    targetLine="union time take time"
//    deleteDir.deletedir(islocal,master,output+targetLine)
//    selectLine(sc,input,targetLine,output+targetLine)

//    targetLine="all edges sum to"
//    deleteDir.deletedir(islocal,master,output+targetLine)
//    selectLine(sc,input,targetLine,output+targetLine)
//
//    targetLine="step"
//    deleteDir.deletedir(islocal,master,output+targetLine)
//    selectLine(sc,input,targetLine,output+targetLine)
//
//    deleteDir.deletedir(islocal,master,output+"step_clean")
//    removestar(sc,output+"step/part-00000",output+"step_clean")
  }
}

case class Edge(src:Int,dst:Int,label:Int) extends Ordered[Edge] with Serializable{
  override def compare(b: Edge): Int = {
    if(this.src>b.src) 1
    else if(this.src<b.src) -1
    else{
      if(this.dst>b.dst) 1
      else if(this.dst<b.dst) -1
      else {
        if(this.label>b.label) 1
        else -1
      }
    }
  }
}
