/**
  * Created by cycy on 2018/1/26.
  */
import scala.collection.JavaConversions._
object test {
  def main(args: Array[String]): Unit = {
    val total_length=100
    val interval=99
    val list:List[Int]=(0 to 99).toList
    for(i:Int<-0 until (total_length,interval)){
      println(list.subList(i,Math.min(i+interval,list.length)).mkString("\t"))
    }
  }
}
