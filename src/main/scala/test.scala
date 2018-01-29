/**
  * Created by cycy on 2018/1/26.
  */
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import utils.HBase_OP

import scala.collection.JavaConversions._
object test {
  def main(args: Array[String]): Unit = {
    HBase_OP.createHBase_Table("a",10)
  }
}
