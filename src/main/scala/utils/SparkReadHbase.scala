package utils

/**
  * Created by cycy on 2018/1/26.
  */
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.spark.{SparkConf, SparkContext}

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

    var htable_name:String="edges"
    var HRegion_splitnum:Int=1000

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

        case "htable_name"=>htable_name=argvalue
        case "HRegion_splitnum"=>HRegion_splitnum=argvalue.toInt

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
    val split_length=utils.HBase_OP.getIntBit(HRegion_splitnum)
    usersRDD.map(s=>{
      val str=Bytes.toString(s._2.getRow)
      (str.substring(split_length,split_length+7).toInt+"\t"+str.substring(split_length+7,split_length+14)
        .toInt+"\t"+str.substring(split_length+14,split_length+16).toInt)
    }).repartition(1).saveAsTextFile(output)
    // =================================
  }
}