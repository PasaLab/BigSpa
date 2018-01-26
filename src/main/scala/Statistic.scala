/**
  * Created by cycy on 2018/1/17.
  */
import org.apache.hadoop.conf.Configuration
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.hadoop.fs.Path
import Graspan_noBF._
import HBase_OP._
object Statistic {
  def selectLine(sc:SparkContext,input:String,targetline:String,output:String): Unit ={
    val data=sc.textFile(input).zipWithIndex().map(s=>s.swap).sortByKey().filter(s=> !s._2.trim.equals("")&&s._2.contains
    (targetline)).map(s=>(s._2.split
    (":"))(1).trim.toInt).zipWithIndex().map(s=>s.swap).sortByKey().map(s=>s._2).repartition(1)
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

    var input: String = "H:/Graspan资料/Graspan_sorce/data/InputGraph/Apache_Httpd_2.2.18_Points-to/Apache_httpd_2.2.18_pointsto_graph"
    var input_grammar: String = "H:/Graspan资料/Graspan数据和源代码/Grammar_Files/rules_pointsto"

    var input1_1: String = "data/nosplit_1/step1/part-00000"
    var input1_2: String = "data/nosplit_1/step2/part-00000"
    var input1_3: String = "data/nosplit_1/step3/part-00000"
    var input1_4: String = "data/nosplit_1/step4/part-00000"
    var output: String = "data/result/step_remove2" //除去ip地址

    val input_origin="data/correctresult/origin/part-00000"
    val input_correct_4="data/correctresult/step4/part-00000"
    val input_correct_3="data/correctresult/step3/part-00000"
    val input_correct_2="data/correctresult/step2/part-00000"
    val input_correct_1="data/correctresult/step1/part-00000"

    var output_M: String = "data/result/M" //除去ip地址
    var output_Mq: String = "data/result/Mq" //除去ip地址
    var par: Int = 4

    val conf = new SparkConf().set("spark.kryoserializer.buffer.max", "128")
    if (islocal) {
      //test location can be adjusted or not
      conf.setAppName("Graspan")
      System.setProperty("hadoop.home.dir", "F:/hadoop-2.6.0/")
      conf.setMaster("local")
    }

    val sc = new SparkContext(conf)
    val step3=sc.textFile(input1_3).map(s=>s.split("\\s+"))
      .map(s=>(s(0).toInt,s(1).toInt,s(2).toInt)).distinct()
    println(step3.map(s=>(s._3,1)).groupByKey().map(s=>(s._1,s._2.sum)).collect().mkString("\n"))

//    val correct_M:RDD[(VertexId,VertexId,EdgeLabel)]=sc.textFile(input_correct_4).map(s=>s.split("\\s+"))
//      .map(s=>(s(0).toInt,s(1).toInt,s(2).toInt)).distinct().filter(s=>s._3==0&&s._1!=s._2)
//    println("M: "+correct_M.count)
//    deleteDir.deletedir(islocal,master,output_M)
//    correct_M.map(s=>(s._1+"\t"+s._2+"\t"+s._3)).repartition(1).saveAsTextFile(output_M)
//
//
//    val correct_Mq:RDD[(VertexId,VertexId,EdgeLabel)]=sc.textFile(input_correct_4).map(s=>s.split("\\s+"))
//      .map(s=>(s(0).toInt,s(1).toInt,s(2).toInt)).distinct().filter(s=>s._3==8&&s._1!=s._2)
//    println("Mq: "+correct_Mq.count)
//    deleteDir.deletedir(islocal,master,output_Mq)
//    correct_Mq.map(s=>(s._1+"\t"+s._2+"\t"+s._3)).repartition(1).saveAsTextFile(output_Mq)
//    val step2:RDD[(VertexId,VertexId,EdgeLabel)]=sc.textFile(input1_2).map(s=>s.split("\\s+"))
//      .map(s=>(s(0).toInt,s(1).toInt,s(2).toInt)).distinct()
//    val step3:RDD[(VertexId,VertexId,EdgeLabel)]=sc.textFile(input1_3).map(s=>s.split("\\s+"))
//      .map(s=>(s(0).toInt,s(1).toInt,s(2).toInt)).distinct()
//    val correct=sc.textFile(input_correct).map(s=>s.split("\\s+"))
//      .map(s=>(s(0).toInt,s(1).toInt,s(2).toInt)).distinct()
//    val res1:RDD[(VertexId,VertexId,EdgeLabel)]=sc.textFile(input1).map(s=>s.split("\\s+"))
//      .map(s=>(s(0).toInt,s(1).toInt,s(2).toInt)).distinct()
//      .sortBy (s=>Edge(s._1,s._2,s._3))
//    val res2:RDD[(VertexId,VertexId,EdgeLabel)]=sc.textFile(input2).map(s=>s.split("\\s+"))
//      .map(s=>(s(0).toInt,s(1).toInt,s(2).toInt)).distinct()
//      .sortBy (s=>Edge(s._1,s._2,s._3))
//    val subset1=res1.subtract(res2)
//    val subset2=res2.subtract(res1)
//    println(subset1.count()+" "+subset2.count)
//    println(sc.textFile(input).filter(s=>s.trim!="").map(s=>s.split("\\s+")).map(s=>s(2).trim).distinct().collect()
//      .mkString
//    ("\n"))
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
