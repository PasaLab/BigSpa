package ONLINE.utils_ONLINE

/**
  * Created by cycy on 2019/1/19.
  */
object Param_pt {
  var islocal: Boolean = true
  var master: String = "local"

  var input_grammar: String = "data/test_grammar"
  var input_graph:String="data/test_graph"
  var output: String = "data/result/" //除去ip地址
  var hbase_output:String="data/result/hbase/hbhfile/"
  var checkpoint_output:String="data/checkpoint"
  var defaultpar:Int=352
  var clusterpar:Int=352
  var newnum_interval:Int=40000000
  var checkpoint_interval:Int=5
  var newedges_interval:Int=40000000

  var openBloomFilter:Boolean=false
  var edges_totalnum:Int=1
  var error_rate:Double=0.1

  var updateRedis_interval:Int=50000
  var queryRedis_interval:Int=50000

  var is_complete_loop:Boolean=false
  var max_complete_loop_turn:Int=5
  var max_delta:Int=10000

  var file_index_f:Int= -1
  var file_index_b:Int= -1

  var check_edge:Boolean=false
  var outputdetails:Boolean=false
  var output_Par_INFO:Boolean=false

  var input_interval:Int=1000
  var changemode_interval:Int=1000
  var add:String="data/httpd_pt_2_add"
  def makeParams(args:Array[String]) {
    for (arg <- args) {
      val argname = arg.split(",")(0)
      val argvalue = arg.split(",")(1)
      argname match {
        case "islocal" => islocal = argvalue.toBoolean
        case "master" => master = argvalue

        case "input_grammar" => input_grammar = argvalue
        case "input_graph" => input_graph = argvalue
        case "output" => output = argvalue
        case "hbase_output" => hbase_output = argvalue
        case "clusterpar" => clusterpar = argvalue.toInt
        case "defaultpar" => defaultpar = argvalue.toInt
        case "newedges_interval" => newedges_interval = argvalue.toInt

        case "openBloomFilter" => openBloomFilter = argvalue.toBoolean
        case "edges_totalnum" => edges_totalnum = argvalue.toInt
        case "error_rate" => error_rate = argvalue.toDouble

        case "updateRedis_interval" => updateRedis_interval = argvalue.toInt
        case "queryRedis_interval" => queryRedis_interval = argvalue.toInt

        case "is_complete_loop" => is_complete_loop = argvalue.toBoolean
        case "max_complete_loop_turn" => max_complete_loop_turn = argvalue.toInt
        case "max_delta" => max_delta = argvalue.toInt

        case "newnum_interval" => newnum_interval = argvalue.toInt
        case "checkpoint_interval" => checkpoint_interval = argvalue.toInt
        case "checkpoint_output" => checkpoint_output = argvalue
        case "file_index_f" => file_index_f = argvalue.toInt
        case "file_index_b" => file_index_b = argvalue.toInt

        case "check_edge" => check_edge = argvalue.toBoolean
        case "outputdetails" => outputdetails = argvalue.toBoolean
        case "output_Par_INFO" => output_Par_INFO = argvalue.toBoolean
        case "input_interval"=>input_interval=argvalue.toInt
        case "changemode_interval"=>changemode_interval=argvalue.toDouble.toInt
        case "add"=>add=argvalue
        case _ => {}
      }
    }
  }

  /**
    * Node_Info 更新参数
    */
  val f=0
  val b=1
  val dir=0//直接计数索引
  val tri=1//三角计数索引
}
