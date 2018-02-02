/**
  * Created by cycy on 2018/1/17.
  */
import org.apache.hadoop.conf.Configuration
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.hadoop.fs.Path
import Graspan_noBF._
import utils.HBase_OP._
import utils.deleteDir
object Statistic {
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
    val data=sc.textFile(input).filter(s=> !s.trim.equals("")).zipWithIndex().filter(s=>s._2%internal==0).map(s=>s
      ._1).repartition(1).saveAsTextFile(output)
  }

  def Edge2String(edge:(VertexId,VertexId,EdgeLabel),
                  nodes_num_bitsize:Int,symbol_num_bitsize:Int,htable_split_Map:Map[Int,String],
                  htable_nodes_interval:Int)
  :String={
    val src_str = filling0(edge._1, nodes_num_bitsize)
    val dst_str = filling0(edge._2, nodes_num_bitsize)
    val label_str = filling0(edge._3, symbol_num_bitsize)
    htable_split_Map.getOrElse(edge._1/htable_nodes_interval,"A")+src_str + dst_str + label_str
  }

  def main(args: Array[String]): Unit = {
    var islocal: Boolean = true
    var master: String = "local"

//    var input_graph: String = "H:/Graspan资料/Graspan_sorce/data/InputGraph/Apache_Httpd_2.2
// .18_Points-to/Apache_httpd_2.2.18_pointsto_graph"
    var input_grammar: String = "H:/Graspan资料/Graspan数据和源代码/Grammar_Files/rules_pointsto"

    var input:String="data/PQSL_df.log"
    var output: String = "data/result/" //除去ip地址

    var par: Int = 4

    val conf = new SparkConf().set("spark.kryoserializer.buffer.max", "128")
    if (islocal) {
      //test location can be adjusted or not
      conf.setAppName("Graspan")
      System.setProperty("hadoop.home.dir", "F:/hadoop-2.6.0/")
      conf.setMaster("local")
    }

    val sc = new SparkContext(conf)

    var targetLine:String="clousure compute take time"
    deleteDir.deletedir(islocal,master,output+targetLine)
    selectLine(sc,input,targetLine,output+targetLine)

    targetLine="newedges_dup"
    deleteDir.deletedir(islocal,master,output+targetLine)
    selectLine(sc,input,targetLine,output+targetLine)

    targetLine="newedges_removedup"
    deleteDir.deletedir(islocal,master,output+targetLine)
    selectLine(sc,input,targetLine,output+targetLine)

    targetLine="distinct take time"
    deleteDir.deletedir(islocal,master,output+targetLine)
    selectLine(sc,input,targetLine,output+targetLine)

    targetLine="update Hbase take time"
    deleteDir.deletedir(islocal,master,output+targetLine)
    selectLine(sc,input,targetLine,output+targetLine)

    targetLine="all edges sum to"
    deleteDir.deletedir(islocal,master,output+targetLine)
    selectLine(sc,input,targetLine,output+targetLine)

    targetLine="step"
    deleteDir.deletedir(islocal,master,output+targetLine)
    selectLine(sc,input,targetLine,output+targetLine)

    deleteDir.deletedir(islocal,master,output+"step_clean")
    removestar(sc,output+"step/part-00000",output+"step_clean")
  }
}

case class Edge(src:VertexId,dst:VertexId,label:EdgeLabel) extends Ordered[Edge] with Serializable{
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
