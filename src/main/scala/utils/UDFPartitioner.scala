package utils
/**
  * Created by cycy on 2018/2/6.
  */
import org.apache.spark.Partitioner
class UDFPartitioner(numParts: Int) extends Partitioner  {

  override def numPartitions: Int = numParts

  override def getPartition(key: Any): Int = {
    key.asInstanceOf[Int]
  }

  override def equals(other: Any): Boolean = other match {
    case udfpar: UDFPartitioner =>
      udfpar.numPartitions == numPartitions
    case _ =>
      false
  }
}