/**
  * Created by cycy on 2018/1/26.
  */

import java.{lang, util}

import it.unimi.dsi.fastutil.longs.{LongArrayList, LongComparator, LongOpenHashSet}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future, Promise}
import scala.util.{Failure, Success}
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

object test {
  def sleep(duration: Long) {
    Thread.sleep(duration)
  }
  def computation(array:Array[Int],f:Int,b:Int): Int = {
    var sum=0
    for(i<-f to b)
      sum+=array(i)
    sum
  }
  def main(args: Array[String]): Unit = {

    val len=10000
    val array=Array.range(0,len)
    val executorService:ExecutorService = Executors.newFixedThreadPool(8)

    var t0=System.nanoTime()
    var t1=System.nanoTime()
    var sum=0


    t0=System.nanoTime()
    array.map(a=>{
      executorService.submit(new Runnable {
        override def run(): Unit = print(a)
      })
    })
    executorService.shutdown()
    t1=System.nanoTime()
    println(s"muitithreads iterates uses "+(t1-t0)) // 输出：List(f1, f2, 2342)


    t0=System.nanoTime()
    array.foreach(print)
    t1=System.nanoTime()
    println(s"serialization sum= $sum uses "+(t1-t0))




//    Await.result(theFutures, Duration.Inf)

  }
}

