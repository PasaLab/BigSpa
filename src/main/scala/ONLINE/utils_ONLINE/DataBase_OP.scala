package ONLINE.utils_ONLINE

import org.apache.spark.rdd.RDD

/**
  * Created by cycy on 2018/8/26.
  */
trait DataBase_OP {

//  def Query_PT(res_edges_maynotin:Array[Array[Int]]):Array[Array[Int]]//HBase
  def Query_PT(edges: RDD[(Int,Int,Int)]):RDD[(Int,Int,Int)]//Redis
  def Query_DF(n:Array[Long]):Array[Array[Int]]

  def Update(edge_processed:RDD[(Int,Int,Int)]):Unit
}
