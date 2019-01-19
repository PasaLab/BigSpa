package utils

/**
  * Created by cycy on 2018/9/12.
  */
object Property extends Serializable {
    var symbol_Map:Map[String, Int]=Map()
    var symbol_num:Int=0
    var symbol_num_bitsize:Int=0
    var loop:List[Int]=List()
    var directadd: Map[Int, Int]=Map()
    var grammar:Array[Array[Int]]=Array()
    var nodes_num_bitsize:Int=0
    var origin_nodes_num:Int=0
    var nodes_totalnum:Int=0
}
