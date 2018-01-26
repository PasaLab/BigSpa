/**
  * Created by cycy on 2018/1/26.
  */
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.hbase.client._
import org.apache.spark.rdd.RDD

object SparkReadHbase {
  def convertScanToString(scan: Scan) = {
    val proto = ProtobufUtil.toScan(scan)
    Base64.encodeBytes(proto.toByteArray)
  }

  def main(args: Array[String]) {
    var islocal: Boolean = true
    var master: String = "local"

    var input_grammar: String = "data/GrammarFiles/test_grammar"
    var input_graph:String="data/InputGraph/test_graph"
    var output: String = "data/result/" //除去ip地址
    var hbase_output:String="data/result/hbase/hbhfile"
    var par: Int = 96

    var openBloomFilter:Boolean=true
    var edges_totalnum:Int=1
    var error_rate:Double=0.1

    var htable_name:String="edges"
    var Hbase_interval:Int=1000
    var query_internal:Int=2

    var is_complete_loop:Boolean=false
    var max_complete_loop_turn:Int=5
    var max_delta:Int=10000

    for (arg <- args) {
      val argname = arg.split(",")(0)
      val argvalue = arg.split(",")(1)
      argname match {
        case "islocal" => islocal = argvalue.toBoolean
        case "master" => master = argvalue

        case "input_grammar" => input_grammar = argvalue
        case "input_graph"=>input_graph=argvalue
        case "output" => output = argvalue
        case "hbase_output"=>hbase_output=argvalue
        case "par" => par = argvalue.toInt

        case "openBloomFilter"=>openBloomFilter=argvalue.toBoolean
        case "edges_totalnum"=>edges_totalnum=argvalue.toInt
        case "error_rate"=>error_rate=argvalue.toDouble

        case "htable_name"=>htable_name=argvalue
        case "Hbase_interval"=>Hbase_interval=argvalue.toInt
        case "query_internal"=>query_internal=argvalue.toInt

        case "is_complete_loop"=>is_complete_loop=argvalue.toBoolean
        case "max_complete_loop_turn"=>max_complete_loop_turn=argvalue.toInt
        case "max_delta"=>max_delta=argvalue.toInt

        case _ => {}
      }
    }

    val conf = new SparkConf().set("spark.kryoserializer.buffer.max", "1024")
    val sc = new SparkContext(conf)
    val h_conf = HBaseConfiguration.create()
    h_conf.set("hbase.zookeeper.quorum", "slave001,slave002,slave003")
    h_conf.setInt("hbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily", 4096)
    h_conf.set(TableOutputFormat.OUTPUT_TABLE, htable_name)


    // ======Load RDD from HBase========
    // use `newAPIHadoopRDD` to load RDD from HBase
    //直接从 HBase 中读取数据并转成 Spark 能直接操作的 RDD[K,V]

    //设置查询的表名
    h_conf.set(TableInputFormat.INPUT_TABLE, htable_name)

    //添加过滤条件，年龄大于 18 岁
    val scan = new Scan()
    h_conf.set(TableInputFormat.SCAN,convertScanToString(scan))

    val usersRDD = sc.newAPIHadoopRDD(h_conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

    val count = usersRDD.count()
    println("Users RDD Count:" + count)
    usersRDD.cache()

    deleteDir.deletedir(islocal,master,output)
    usersRDD.map(s=>{
      val str=Bytes.toString(s._2.getRow)
      (str.substring(1,8).toInt+"\t"+str.substring(8,15).toInt+"\t"+str.substring(15,17).toInt)
    }).repartition(1).saveAsTextFile(output)
    // =================================
  }
}