package OFFLINE.utils_OFFLINE

/**
  * Created by cycy on 2019/1/19.
  */
object Param_df {
  var islocal: Boolean = true
  var master: String = "local"

  var input_e:String="data/test_graph"
  var input_n:String="data/test_graph"
  var output: String = "data/result/" //除去ip地址
  var hbase_output:String="data/result/hbase/hbhfile/"
  var checkpoint_output:String="data/checkpoint"
  var defaultpar:Int=384
  var clusterpar:Int=384
  var newnum_interval:Int=40000000
  var checkpoint_interval:Int=10
  var newedges_interval:Int=40000000

  var updateRedis_interval:Int=50000
  var queryRedis_interval:Int=50000

  var is_complete_loop:Boolean=false
  var max_complete_loop_turn:Int=5
  var max_convergence_loop:Int=100

  var file_index_f:Int= -1
  var file_index_b:Int= -1

  var check_edge:Boolean=false
  var convergence_threshold:Int=10000
  var output_Par_INFO:Boolean=false

  def makeParams(args:Array[String]) {
    for (arg <- args) {
      val argname = arg.split(",")(0)
      val argvalue = arg.split(",")(1)
      argname match {
        case "islocal" => islocal = argvalue.toBoolean
        case "master" => master = argvalue

        case "input_e" => input_e = argvalue
        case "input_n" => input_n = argvalue
        case "output" => output = argvalue
        case "hbase_output" => hbase_output = argvalue
        case "clusterpar" => clusterpar = argvalue.toInt
        case "defaultpar" => defaultpar = argvalue.toInt
        case "newedges_interval" => newedges_interval = argvalue.toInt

        case "queryRedis_interval" => queryRedis_interval = argvalue.toInt
        case "updateRedis_interval" => updateRedis_interval = argvalue.toInt

        case "is_complete_loop" => is_complete_loop = argvalue.toBoolean
        case "max_complete_loop_turn" => max_complete_loop_turn = argvalue.toInt
        case "max_convergence_loop" => max_convergence_loop = argvalue.toInt

        case "newnum_interval" => newnum_interval = argvalue.toInt
        case "checkpoint_interval" => checkpoint_interval = argvalue.toInt
        case "checkpoint_output" => checkpoint_output = argvalue
        case "file_index_f" => file_index_f = argvalue.toInt
        case "file_index_b" => file_index_b = argvalue.toInt

        case "check_edge" => check_edge = argvalue.toBoolean
        case "convergence_threshold" => convergence_threshold = argvalue.toInt
        case "output_Par_INFO" => output_Par_INFO = argvalue.toBoolean
        case _ => {}
      }
    }
  }
}
