package utils

import java.util
import java.util.Scanner

import org.apache.spark.{HashPartitioner, RangePartitioner, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileUtil, Path}
import org.apache.spark.util.LongAccumulator

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer
import it.unimi.dsi.fastutil.ints._
import it.unimi.dsi.fastutil.longs.{LongArrayList, LongComparator, LongOpenHashSet}
import BigSpa_OP_java.getCorrectLength
/**
  * Created by cycy on 2018/1/29.
  */


object BIgSpa_OP  {

  /**
    *对原来的Linux dataflow文件进行处理，分成n和e两部分，不在正式程序的预处理中
    */
  def processDF(sc: SparkContext, input_graph: String, output: String, par: Int): Unit = {
    val graph = sc.textFile(input_graph, par).filter(s => s.trim != "").map(s => s.split("\\s+").map(_.trim))
    graph.filter(_ (2) == "e").map(s => (s(0).toInt, s(1).toInt)).groupByKey().repartition(1).sortByKey().map(s => (s._1 + ":" + s._2.mkString("\t")))
      .saveAsTextFile(output + "/e")
    //    graph.filter(_(2)=="e").map(s=>s(0)+"\t"+s(1)+"\t1").repartition(1).saveAsTextFile(output+"/e")
    graph.filter(_ (2) == "n").map(s => s(0) + "\t" + s(1) + "\t0").repartition(1).saveAsTextFile(output + "/n")
  }


  /**
    * ****************************************************************************************************************
    * 预处理 操作
    * ****************************************************************************************************************
    */
  //  def processLinux(sc: SparkContext, input_graph: String, input_grammar: String, output: String, par: Int): Unit = {
  //    val configuration = new Configuration()
  //    val input = new Path(input_graph)
  //    val hdfs = input.getFileSystem(configuration)
  //    val fs = hdfs.listStatus(input)
  //    val fileName = FileUtil.stat2Paths(fs)
  //
  //    var start = 0L
  //    for (i <- fileName) {
  //      println(i.toString + " is processing")
  //      println("start:     \t" + start)
  //      val tmp_graph = sc.textFile(i.toString, par).filter(!_.trim.equals("")).map(s => {
  //        val str = s.split("\\s+")
  //        (str(0).toInt, str(1).toInt, str(2))
  //      })
  //      val nodes_order = tmp_graph.flatMap(s => List(s._1, s._2)).distinct()
  //      println("nodes num: \t" + nodes_order.count())
  //
  //      val all = tmp_graph.map(s => (s._1 + start, s._2 + start, s._3))
  //      val rdd = all.flatMap(s => List(s._1, s._2)).distinct()
  //      println("transfer:  \t" + rdd.count)
  //      println("min : " + rdd.min())
  //      println("max : " + rdd.max())
  //
  //      all.map(s => (s._1 + "\t" + s._2 + "\t" + s._3)).repartition(1)
  //        .saveAsTextFile(output + "/" + i.toString.split("/").last)
  //      start = start + nodes_order.count()
  //      println("start:     \t" + start)
  //    }
  //  }
  def getIntBit(a:Int):Int={
    var tmp=a
    var num=0
    while(tmp>0){
      tmp/=10
      num+=1
    }
    num
  }
  def processGrammar(grammar_origin: List[Array[String]], input_grammar: String)
  : (Map[String, Int], Int, Int, List[Int], Map[Int, Int], Array[Array[Int]]) = {
    val symbol_Map = grammar_origin.flatMap(s => s.toList).distinct.zipWithIndex.toMap
    val (loop: List[Int], directadd: Map[Int, Int], grammar:Array[Array[Int]]) = {
      //        println("Grammar need preprocessed")
      (grammar_origin.filter(s => s.length == 1).map(s => symbol_Map.getOrElse(s(0), -1)), grammar_origin.filter(s => s
        .length == 2)
        .map(s => (symbol_Map.getOrElse(s(1), -1), symbol_Map.getOrElse(s(0), -1))).toMap, grammar_origin.filter(s => s
        .length == 3).map(s => ((symbol_Map.getOrElse(s(1), -1), symbol_Map.getOrElse(s(2), -1)), symbol_Map.getOrElse(s(0)
        , -1))).toArray.map(x => Array(x._1._1, x._1._2, x._2)))
    }
    val symbol_num = symbol_Map.size
    val symbol_num_bitsize = getIntBit(symbol_num)
    (symbol_Map, symbol_num, symbol_num_bitsize, loop, directadd, grammar)
  }
  def processGraph(sc: SparkContext, input_graph: String, file_index_f: Int, fine_index_b: Int, input_grammar: String,
                   symbol_Map: Map[String, Int],
                   loop: List[Int],
                   directadd: Map[Int, Int], par: Int): (RDD[Array[Int]], Int, Int) = {
    val input_str: String = {
      if (input_graph.contains("Linux")) {
        val configuration = new Configuration()
        val input = new Path(input_graph)
        val hdfs = input.getFileSystem(configuration)
        val fs = hdfs.listStatus(input)
        val fileName = FileUtil.stat2Paths(fs)
        var tmpstr: String = fileName(file_index_f).toString
        println(tmpstr)
        for (i <- file_index_f + 1 to fine_index_b) {
          println(fileName(i))
          tmpstr += "," + fileName(i)
        }
        tmpstr
      }
      else input_graph
    }
    val graph_changelabel: RDD[Array[Int]] =
      sc.textFile(input_str, par).filter(!_.trim.equals("")).map(s => {
        val str = s.split("\\s+")
        Vector(str(0).toInt, str(1).toInt, symbol_Map.getOrElse(str(2), -1))
      })
        .distinct.map(_.toArray)
        .setName("graph_changelabel").cache()
    println("graph_origin: " + graph_changelabel.count())
    val nodes = graph_changelabel.flatMap(s => List(s(0), s(1))).distinct()
    var graph = {
      println("Graph need preprocessed")
      (graph_changelabel
        ++ nodes.flatMap(s => loop.map(x => Array(s, s, x)))
        ++ graph_changelabel.filter(s => directadd.contains(s(2))).map(s => Array(s(0), s(1), directadd.getOrElse(s(2), -1)))
        )
    }.cache()
    if (graph.filter(s => s(2) == -1).isEmpty() == false) println("读取Int出错")
    val graph_filter = graph.filter(s => s(2) != -1)
    val nodes_totalnum = nodes.count().toInt
    val nodes_num_bitsize = getIntBit(nodes_totalnum)
    (graph_filter, nodes_num_bitsize, nodes_totalnum)
  }

  def processGrammar_Split(grammar_origin: List[Array[String]], input_grammar: String) = {
    val symbol_Map = grammar_origin.flatMap(s => s.toList).distinct.zipWithIndex.toMap
    val (loop: List[Int], directadd: Map[Int, Int], grammar:Array[Array[Int]]) = {
      //        println("Grammar need preprocessed")
      (grammar_origin.filter(s => s.length == 1).map(s => symbol_Map.getOrElse(s(0), -1)), grammar_origin.filter(s => s
        .length == 2)
        .map(s => (symbol_Map.getOrElse(s(1), -1), symbol_Map.getOrElse(s(0), -1))).toMap, grammar_origin.filter(s => s
        .length == 3).map(s => ((symbol_Map.getOrElse(s(1), -1), symbol_Map.getOrElse(s(2), -1)), symbol_Map.getOrElse(s(0)
        , -1))).toArray.map(x => Array(x._1._1, x._1._2, x._2)))
    }
    val symbol_num = symbol_Map.size
    val symbol_num_bitsize = getIntBit(symbol_num)
    Property.symbol_Map=symbol_Map
    Property.symbol_num=symbol_num
    Property.symbol_num_bitsize=symbol_num_bitsize
    Property.loop=loop
    Property.directadd=directadd
    Property.grammar=grammar
  }
  def processGraph_Split(sc: SparkContext, input_graph: String, file_index_f: Int, fine_index_b: Int, input_grammar:
  String, symbol_Map: Map[String, Int], loop: List[Int], directadd: Map[Int, Int], par: Int)
  : RDD[Array[Int]]= {
    val input_str: String = {
      if (input_graph.contains("Linux")) {
        val configuration = new Configuration()
        val input = new Path(input_graph)
        val hdfs = input.getFileSystem(configuration)
        val fs = hdfs.listStatus(input)
        val fileName = FileUtil.stat2Paths(fs)
        var tmpstr: String = fileName(file_index_f).toString
        println(tmpstr)
        for (i <- file_index_f + 1 to fine_index_b) {
          println(fileName(i))
          tmpstr += "," + fileName(i)
        }
        tmpstr
      }
      else input_graph
    }
    val graph_changelabel: RDD[Array[Int]] =
      sc.textFile(input_str, par).filter(!_.trim.equals("")).map(s => {
        val str = s.split("\\s+")
        Vector(str(0).toInt, str(1).toInt, symbol_Map.getOrElse(str(2), -1))
      })
        .distinct.map(_.toArray)
        .setName("graph_changelabel").cache()
    println("graph_origin: " + graph_changelabel.count())
    val nodes = graph_changelabel.flatMap(s => List(s(0), s(1))).distinct()
    var graph = {
      println("Graph need preprocessed")
      (graph_changelabel
        ++ nodes.flatMap(s => loop.map(x => Array(s, s, x)))
        ++ graph_changelabel.filter(s => directadd.contains(s(2))).map(s => Array(s(0), s(1), directadd.getOrElse(s(2), -1)))
        )
    }.cache()
    if (graph.filter(s => s(2) == -1).isEmpty() == false) println("读取Int出错")
    val graph_filter = graph.filter(s => s(2) != -1)
    val nodes_totalnum = nodes.count().toInt
    val nodes_num_bitsize = getIntBit(nodes_totalnum)
    Property.nodes_totalnum=nodes_totalnum
    Property.origin_nodes_num=nodes_totalnum
    Property.nodes_num_bitsize=nodes_num_bitsize
    graph_filter
  }


  def getLinux_input_EandN(input_e: String, input_n: String, file_index_f: Int, file_index_b: Int, master: String):
  (String, String) = {
    val configuration = new Configuration()
    println("get input e")
    var input = new Path(input_e)
    var hdfs = input.getFileSystem(configuration)
    var fs = hdfs.listStatus(input)
    var fileName = FileUtil.stat2Paths(fs)
    var tmpstr_e: String = fileName(file_index_f).toString.substring(master.length - 1)
    //    var tmpstr_nomaster:String=fileName(file_index_f).toString.substring(master.length-1)
    println(tmpstr_e)
    for (i <- file_index_f + 1 to file_index_b) {
      println(fileName(i))
      tmpstr_e += "," + fileName(i).toString.substring(master.length - 1)
      //      tmpstr_nomaster+=","+fileName(i).toString.substring(master.length-1)
    }

    println("get input n")
    input = new Path(input_n)
    hdfs = input.getFileSystem(configuration)
    fs = hdfs.listStatus(input)
    fileName = FileUtil.stat2Paths(fs)
    var tmpstr_n: String = fileName(file_index_f).toString.substring(master.length - 1)
    println(tmpstr_n)
    for (i <- file_index_f + 1 to file_index_b) {
      println(fileName(i))
      tmpstr_n += "," + fileName(i).toString.substring(master.length - 1)
    }
    (tmpstr_e, tmpstr_n)
  }

  /**
    * ****************************************************************************************************************
    * Compute 操作
    * ****************************************************************************************************************
    */
  def init_e(index: Int, mid_adj: Iterator[(Int, Array[Int])], master: String, input_e_nomaster: String)
  : Iterator[Int] = {
    Dataflow_e_formation.get_e(master, input_e_nomaster, index)
    Array[Int]().toIterator
  }
  def binary_search(e: Array[(Int, Array[Int])], target: Int, f0: Int): Int = {
    var f = f0
    var b = e.length
    while (b > f + 1) {
      val mid = (b + f) / 2
      if (e(mid)._1 == target) return mid
      if (e(mid)._1 < target) f = mid
      else b = mid
    }
    if (e(f)._1 != target) return -1
    else return f
  }


  def computeInPartition_df(step: Int, index: Int,
                            mid_adj: Iterator[(Int, Array[Int])], master: String, input_e_nomaster: String,
                            nodes_num_bitsize: Int, symbol_num_bitsize: Int,
                            is_complete_loop: Boolean, max_complete_loop_turn: Int,dataBase_OP: DataBase_OP)
  : Iterator[(Int, (Array[Array[Int]], String, Long))] = {
    var t0 = System.nanoTime(): Double
    var t1 = System.nanoTime(): Double
    var recording = ""
    println("At STEP " + step + ", partition " + index)
    recording += "At STEP " + step + ", partition " + index
    val e_edges = Dataflow_e_formation.get_e(master, input_e_nomaster, index)
    //    println("get e: "+e_edges.length)
    val new_n = new LongArrayList()
    val long_tocheck = new LongArrayList()
    var coarest_num = 0L
    val n = mid_adj.toArray.sortBy(_._1)
    var index_n = 0
    val len_e = e_edges.length
    var len_n = n.length
    var f0 = 0
    while (index_n < len_n) {
      val index_e = binary_search(e_edges, n(index_n)._1, f0)
      if (index_e != -1) {
        val res = BigSpa_OP_java.join_df( n(index_n)._2, e_edges(index_e)._2)
        new_n.addElements(new_n.length, res, 0, res.length)
        f0 = index_e
      }
      index_n += 1
    }
    coarest_num = new_n.length
    if (is_complete_loop == false) {
      println("No Loop")
      t1 = System.nanoTime(): Double
      val toolong = {
        if ((t1 - t0) / 1000000000.0 < 10) "normal"
        else if ((t1 - t0) / 1000000000.0 < 100) "longer than 10"
        else "longer than 100"
      }
      println()
      println("At STEP " + step + ", partition " + index+
        " , ||"
        + " origin_formedges: " + coarest_num + " ||"
        + "join take time: " + toolong + ", " + ((t1 - t0) / 1000000000.0) + " secs")
      recording += ("|| "
        + "origin_formedges: " + coarest_num + " ||"
        + "join take time: " + toolong + ", REPARJOIN" + ((t1 - t0) / 1000000000.0) + "REPARJOIN secs")
      //      long_tocheck.appendAll(new_n)
      //      long_tocheck.addElements(long_tocheck.length,new_n.toArray(),0,new_n.size())
    }
    else {
      println("Loop "+ max_complete_loop_turn)
      var new_n_array = {
        val tmp = new LongOpenHashSet(new_n)
        tmp.toLongArray.sorted
      }
      long_tocheck.addElements(long_tocheck.length, new_n_array)
      var turn = 1
      var continue = turn < max_complete_loop_turn
      println("Begin closure")
      while (continue) {
        turn += 1
        len_n = new_n_array.length
        index_n = 0
        f0 = 0
        val tmp_new_n = new LongArrayList()
        while (index_n < len_n) {
          val index_e = binary_search(e_edges, ((new_n_array(index_n) >> 32)).toInt, f0)
          if (index_e != -1) {
            val n_end = new Array[Int](1)
            val tmp = BigSpa_OP_java.join_df_loop(e_edges(index_e)._1, new_n_array, e_edges(index_e)._2,
              index_n, n_end)
            tmp_new_n.addElements(tmp_new_n.length, tmp, 0, tmp.length)
            f0 = index_e
            //            println("index_n: "+n_end(0))
            index_n = n_end(0)
          }
          else {
            var i = index_n
            val flag = new_n_array(index_n) >>> 32
            while (i < len_n && (new_n_array(i) >>> 32) == flag) i += 1
            index_n = i
          }
        }
        //        println("produce edges "+tmp_new_n.size())
        if (turn >= max_complete_loop_turn || tmp_new_n.length > 100000) continue = false

        new_n_array = {
          val tmp = new LongOpenHashSet(tmp_new_n)
          tmp.toLongArray.sorted
        }
        long_tocheck.addElements(long_tocheck.length, new_n_array)
      }
      println("all turns "+turn)
      coarest_num = long_tocheck.size()
      t1 = System.nanoTime(): Double
      val toolong = {
        if ((t1 - t0) / 1000000000.0 < 10) "normal"
        else if ((t1 - t0) / 1000000000.0 < 100) "longer than 10"
        else "longer than 100"
      }
      println()
      println("At STEP " + step + ", partition " + index+
        " , ||"
        + " origin_formedges: " + coarest_num + " ||"
        + "join take time: " + toolong + ", " + ((t1 - t0) / 1000000000.0) + " secs")
      recording += ("|| "
        + "origin_formedges: " + coarest_num + " ||"
        + "join take time: " + toolong + ", REPARJOIN" + ((t1 - t0) / 1000000000.0) + "REPARJOIN secs")
    }

    /**
      * 多线程开启
      */
    //    val executors = Executors.newCachedThreadPool()
    //    val thread = new MyThread
    //    class MyThread extends Thread{
    //      override def run(): Unit = {
    //
    //      }
    //    }
    //    executors.submit(thread)
    /**
      * Redis过滤
      */
    t0 = System.nanoTime(): Double
    val len = long_tocheck.length
    println("long_tocheck: " + len)
    //    if(res_edges_array.filter(s=>s(0)==s(1)&&s(2)==7).length>0) recording += "Target Exist!"
    val res_edges =
    dataBase_OP.Query_DF(long_tocheck.toLongArray())

    //    if(res_edges_array.filter(s=>s(0)==s(1)&&s(2)==7).length>0) recording += "After HBAse Filter Target Exist!"
    t1 = System.nanoTime(): Double
    println("At STEP " + step + ", partition " + index+" , Query Redis for edges: \t" + len
      + ",\ttake time: \t" + ((t1 - t0) / 1000000000.0).formatted("%.3f") + " sec"
      + ", \tres_edges:             \t" + res_edges.length + "\n")
    recording += ("Query DB for edges: \t" + len
      + ",\ttake time: \tREPARDB" + ((t1 - t0) / 1000000000.0).formatted("%.3f") + "REPARDB sec"
      + ", \tres_edges:             \t" + res_edges.length + "\n")
    List((index, (res_edges, recording, coarest_num))).toIterator
    //    List((res_edges_array.toList,recording,coarest_num)).toIterator
  }

  def computeInPartition_pt(step: Int, index: Int,
                            mid_adj: Iterator[(Int, (Array[Int], Array[Int], Array[Int], Array[Int],Array[Int]))],
                            symbol_num: Int,
                            grammar: Array[Array[Int]],
                            nodes_num_bitsize: Int, symbol_num_bitsize: Int,
                            directadd0: Map[Int, Int],
                            dataBase_OP: DataBase_OP)
  : Iterator[(Int, (Array[Array[Int]], String, Long))] = {
    var recording = ""
    val res_edges_list = new IntArrayList()
    println("At STEP " + step + ", partition " + index)
    recording += "At STEP " + step + ", partition " + index
    val directadd = directadd0.toArray.map(s => Array(s._1, s._2))
    var coarest_num = 0L
    var t0 = System.nanoTime(): Double
    var t1 = System.nanoTime(): Double
    var time_realjoin = 0.0
    mid_adj.foreach(s => {
      val t00 = System.nanoTime()
      val res = BigSpa_OP_java.join(s._1,
        s._2._1,
        s._2._2,
        s._2._3,
        s._2._4,
        s._2._5,
        grammar, symbol_num, directadd)
      //      coarest_num += res.length
      //      recording :+="*******************************"
      //      recording :+="mid: "+s._1+"\n"
      //      recording :+="old: "+s._2._1.map(x=>"("+"("+x._1+"),"+x._2+")").mkString(", ")+"\n"
      //      recording :+="new: "+s._2._2.map(x=>"("+"("+x._1+"),"+x._2+")").mkString(", ")+"\n"
      //      recording :+="res: "+res.toList.map(x=>"("+"("+x(0)+","+x(1)+"),"+x(2)+")").mkString(", ")+"\n"
      //      recording :+="*******************************"
      //        coarest_num +=res.size()
      res_edges_list.addElements(res_edges_list.length, res, 0, res.length)
      time_realjoin += System.nanoTime() - t00
    })
    coarest_num = res_edges_list.length / 3
    //    var old_edges:List[(Int,Int,Int)]=mid_adj_list.flatMap(s=>(s._2)).map(s=>(s._1._1,s._1._2,s._2)

    t1 = System.nanoTime(): Double
    val toolong = {
      if ((t1 - t0) / 1000000000.0 < 10) "normal"
      else if ((t1 - t0) / 1000000000.0 < 100) "longer than 10"
      else "longer than 100"
    }
    println()
    println("At STEP " + step + ", partition " + index+
      " , ||"
      + " origin_formedges: " + coarest_num
      + " ||"
      + "join take time: " + toolong + ", " + ((t1 - t0) / 1000000000.0) + " secs"
      + " real join take time: " + time_realjoin/ 1000000000.0 + " secs")
    recording += ("|| "
      + "origin_formedges: " + coarest_num
      + " ||"
      + "join take time: " + toolong + ", REPARJOIN" + ((t1 - t0) / 1000000000.0) + "REPARJOIN secs")

    /**
      * 多线程开启
      */
    //    val executors = Executors.newCachedThreadPool()
    //    val thread = new MyThread
    //    class MyThread extends Thread{
    //      override def run(): Unit = {
    //
    //      }
    //    }
    //    executors.submit(thread)
    /**
      * Redis过滤
      */
    t0 = System.nanoTime(): Double
    val len = res_edges_list.length / 3
    //    if(res_edges_array.filter(s=>s(0)==s(1)&&s(2)==7).length>0) recording += "Target Exist!"
    val res_edges =
    dataBase_OP.Query_PT(res_edges_list.toIntArray())

    //    if(res_edges_array.filter(s=>s(0)==s(1)&&s(2)==7).length>0) recording += "After HBAse Filter Target Exist!"
    t1 = System.nanoTime(): Double
    println("At STEP " + step + ", partition " + index+
      " , Query Redis for edges: \t" + len
      + ",\ttake time: \t" + ((t1 - t0) / 1000000000.0).formatted("%.3f") + " sec"
      + ", \tres_edges:             \t" + res_edges.length + "\n")
    recording += ("Query DB for edges: \t" + len
      + ",\ttake time: \tREPARDB" + ((t1 - t0) / 1000000000.0).formatted("%.3f") + "REPARDB sec"
      + ", \tres_edges:             \t" + res_edges.length + "\n")
    List((index, (res_edges, recording, coarest_num))).toIterator
    //    List((res_edges_array.toList,recording,coarest_num)).toIterator
  }

  def computeInPartition_pt_Split(step: Int, index: Int,
                            mid_adj: Iterator[(Int, (Array[Int], Array[Int], Array[Int], Array[Int],Array[Int]))],
                            symbol_num: Int,
                            grammar: Array[Array[Int]],
                            nodes_num_bitsize: Int, symbol_num_bitsize: Int,
                            directadd0: Map[Int, Int],
                            dataBase_OP: Redis_OP,Global_Vertex_Map:ArrayBuffer[(Int,Int,Int)],origin_nodes_num:Int)
  : Iterator[(Int, (Array[Array[Int]], String, Long))] = {
    var recording = ""
    val res_edges_list = new IntArrayList()
    println("At STEP " + step + ", partition " + index)
    recording += "At STEP " + step + ", partition " + index
    val directadd = directadd0.toArray.map(s => Array(s._1, s._2))
    var coarest_num = 0L
    var t0 = System.nanoTime(): Double
    var t1 = System.nanoTime(): Double
    var time_realjoin = 0.0
    mid_adj.foreach(s => {
      val t00 = System.nanoTime()
      val res = BigSpa_OP_java.join(s._1,
        s._2._1,
        s._2._2,
        s._2._3,
        s._2._4,
        s._2._5,
        grammar, symbol_num, directadd)
      //      coarest_num += res.length
      //      recording :+="*******************************"
      //      recording :+="mid: "+s._1+"\n"
      //      recording :+="old: "+s._2._1.map(x=>"("+"("+x._1+"),"+x._2+")").mkString(", ")+"\n"
      //      recording :+="new: "+s._2._2.map(x=>"("+"("+x._1+"),"+x._2+")").mkString(", ")+"\n"
      //      recording :+="res: "+res.toList.map(x=>"("+"("+x(0)+","+x(1)+"),"+x(2)+")").mkString(", ")+"\n"
      //      recording :+="*******************************"
      //        coarest_num +=res.size()
      res_edges_list.addElements(res_edges_list.length, res, 0, res.length)
      time_realjoin += System.nanoTime() - t00
    })
    coarest_num = res_edges_list.length / 3
    //    var old_edges:List[(Int,Int,Int)]=mid_adj_list.flatMap(s=>(s._2)).map(s=>(s._1._1,s._1._2,s._2)

    t1 = System.nanoTime(): Double
    val toolong = {
      if ((t1 - t0) / 1000000000.0 < 10) "normal"
      else if ((t1 - t0) / 1000000000.0 < 100) "longer than 10"
      else "longer than 100"
    }
    println()
    println("At STEP " + step + ", partition " + index+
      " , ||"
      + " origin_formedges: " + coarest_num
      + " ||"
      + "join take time: " + toolong + ", " + ((t1 - t0) / 1000000000.0) + " secs"
      + " real join take time: " + time_realjoin/ 1000000000.0 + " secs")
    recording += ("|| "
      + "origin_formedges: " + coarest_num
      + " ||"
      + "join take time: " + toolong + ", REPARJOIN" + ((t1 - t0) / 1000000000.0) + "REPARJOIN secs")

    /**
      * 多线程开启
      */
    //    val executors = Executors.newCachedThreadPool()
    //    val thread = new MyThread
    //    class MyThread extends Thread{
    //      override def run(): Unit = {
    //
    //      }
    //    }
    //    executors.submit(thread)
    /**
      * Redis过滤
      */
    t0 = System.nanoTime(): Double
    val len = res_edges_list.length / 3
    //    if(res_edges_array.filter(s=>s(0)==s(1)&&s(2)==7).length>0) recording += "Target Exist!"
    val res_edges =
    dataBase_OP.Query_PT_Split(res_edges_list.toIntArray(),Global_Vertex_Map,origin_nodes_num)

    //    if(res_edges_array.filter(s=>s(0)==s(1)&&s(2)==7).length>0) recording += "After HBAse Filter Target Exist!"
    t1 = System.nanoTime(): Double
    println("At STEP " + step + ", partition " + index+
      " , Query Redis for edges: \t" + len
      + ",\ttake time: \t" + ((t1 - t0) / 1000000000.0).formatted("%.3f") + " sec"
      + ", \tres_edges:             \t" + res_edges.length + "\n")
    recording += ("Query DB for edges: \t" + len
      + ",\ttake time: \tREPARDB" + ((t1 - t0) / 1000000000.0).formatted("%.3f") + "REPARDB sec"
      + ", \tres_edges:             \t" + res_edges.length + "\n")
    List((index, (res_edges, recording, coarest_num))).toIterator
    //    List((res_edges_array.toList,recording,coarest_num)).toIterator
  }

  /**
    * ****************************************************************************************************************
    * Union 操作
    * ****************************************************************************************************************
    */

  def Silence_oldedges(old_f_index_list: Array[Int], new_f_index_list: Array[Int], old_b_index_list: Array[Int],
                       new_b_index_list: Array[Int],symbol_num:Int): Boolean = {
//    val symbol_num = old_f_index_list.length
    var isSilence = true
    var i = 0
    while (i < symbol_num && isSilence) {
      if (new_f_index_list(i) != old_f_index_list(i) || new_b_index_list(i) != old_b_index_list(i)) {
        isSilence = false
      }
      i += 1
    }
    isSilence
  }

  /**
    * Innner        Outer
    * |A1|A2|B1|B2……|A1|A2|B1|B2……
    *
    * @param edges
    * @param symbol_num
    * @return
    */
  def Union(edges: Iterator[(Int, (Iterable[(Array[Int], Array[Int], Array[Int], Array[Int], Array[Int])],
    Iterable[Array[Array[Int]]]))], symbol_num: Int): Iterator[(Int, (Array[Int], Array[Int], Array[Int], Array[Int],
    Array[Int]))] = {
    edges.map(s => {
      val flag = s._1
      if (s._2._2.isEmpty) {//没有新边需要合并
      val old = s._2._1.head
        if (Silence_oldedges(old._2, old._3, old._4, old._5,symbol_num)) {//上一次的新边已合并
          (flag, old)
        }
        else {
          //上一次的新边尚未合并，因为没有新边插入，简单合并指针即可
          val old_f_index_list = new Array[Int](symbol_num)
          val new_f_index_list = new Array[Int](symbol_num)
          val old_b_index_list = new Array[Int](symbol_num)
          val new_b_index_list = new Array[Int](symbol_num)
          for (i <- 0 until symbol_num) {
            old_f_index_list(i) = old._3(i)
            new_f_index_list(i) = old._3(i)
            old_b_index_list(i) = old._5(i)
            new_b_index_list(i) = old._5(i)
          }
          (flag, (old._1, old_f_index_list, new_f_index_list, old_b_index_list, new_b_index_list))
        }
      }
      else {
        //新边不为空
        val old: (Array[Int], Array[Int], Array[Int], Array[Int], Array[Int]) = {
          if (s._2._1.isEmpty) {
            val old_f_index_list = new Array[Int](symbol_num)
            val new_f_index_list = new Array[Int](symbol_num)
            val old_b_index_list = new Array[Int](symbol_num)
            val new_b_index_list = new Array[Int](symbol_num)
            for (i <- 0 until symbol_num) {
              old_f_index_list(i) = -1
              new_f_index_list(i) = -1
              old_b_index_list(i) = -1
              new_b_index_list(i) = -1
            }
            (Array[Int](), old_f_index_list, new_f_index_list, old_b_index_list, new_b_index_list)
          }
          else s._2._1.head
        }
        val old_f_index_list = new Array[Int](symbol_num)
        val new_f_index_list = new Array[Int](symbol_num)
        val old_b_index_list = new Array[Int](symbol_num)
        val new_b_index_list = new Array[Int](symbol_num)
        val (new_f_list, new_b_list) = {
          val tmp_new_f_index_list = new Array[Int](symbol_num)
          val tmp_new_b_index_list = new Array[Int](symbol_num)
          val new_e = s._2._2.head
          val new_e_length = new_e.length
          val new_f_list = new Array[ArrayBuffer[Int]](symbol_num)
          val new_b_list = new Array[ArrayBuffer[Int]](symbol_num)
          for (i <- 0 until symbol_num) {
            new_f_list(i) = new ArrayBuffer[Int](new_e_length)
            new_b_list(i) = new ArrayBuffer[Int](new_e_length)
          }
          var index = 0
          while (index < new_e_length) {
            if (new_e(index)(1) == flag) {
              new_f_list(new_e(index)(2)).append(new_e(index)(0))
            }
            if (new_e(index)(0) == flag) {
              new_b_list(new_e(index)(2)).append(new_e(index)(1))
            }
            index += 1
          }
          (new_f_list, new_b_list)
        }
        val alllen = {
          var sum = 0
          for (i <- 0 until symbol_num) {
            sum += new_f_list(i).length + new_b_list(i).length
          }
          sum + old._1.length
        }
        val alledges = {
          val alledges = new Array[Int](alllen)
          System.arraycopy(old._1, 0, alledges, 0, old._3(0) + 1)
          old_f_index_list(0) = old._3(0)
          System.arraycopy(new_f_list(0).toArray, 0, alledges, old_f_index_list(0) + 1, new_f_list(0).length)
          new_f_index_list(0) = old_f_index_list(0) + new_f_list(0).length
          for (i <- 1 until symbol_num) {
            System.arraycopy(old._1, old._3(i - 1) + 1, alledges, new_f_index_list(i - 1) + 1, (old._3(i) - old._3(i - 1)))
            old_f_index_list(i) = new_f_index_list(i - 1) + (old._3(i) - old._3(i - 1))
            System.arraycopy(new_f_list(i).toArray, 0, alledges, old_f_index_list(i) + 1, new_f_list(i).length)
            new_f_index_list(i) = old_f_index_list(i) + new_f_list(i).length
          }
          System.arraycopy(old._1, old._3.last + 1, alledges, new_f_index_list.last + 1, (old._5(0) - old._3.last))
          old_b_index_list(0) = new_f_index_list.last + (old._5(0) - old._3.last)
          System.arraycopy(new_b_list(0).toArray, 0, alledges, new_f_index_list.last + 1, new_b_list(0).length)
          new_b_index_list(0) = old_b_index_list(0) + new_b_list(0).length
          for (i <- 1 until symbol_num) {
            System.arraycopy(old._1, old._5(i - 1) + 1, alledges, new_b_index_list(i - 1) + 1, (old._5(i) - old._5(i - 1)))
            old_b_index_list(i) = new_b_index_list(i - 1) + (old._5(i) - old._5(i - 1))
            System.arraycopy(new_b_list(i).toArray, 0, alledges, old_b_index_list(i) + 1, new_b_list(i).length)
            new_b_index_list(i) = old_b_index_list(i) + new_b_list(i).length
          }
          alledges
        }
        (flag, (alledges, old_f_index_list, new_f_index_list, old_b_index_list, new_b_index_list))
      }

    })
  }

  def union_normal_elements(e:(Int,((Array[Int],Array[Int],Array[Int]),(Array[ArrayBuffer[Int]],
  Array[ArrayBuffer[Int]])))):(Int, (Array[Int], Array[Int], Array[Int], Array[Int], Array[Int]))={
  val flag=e._1
  val symbol_num=e._2._2._1.length
    val new_f_list:Array[ArrayBuffer[Int]]=e._2._2._1
    val new_b_list:Array[ArrayBuffer[Int]]=e._2._2._2
    val old_f_index_list=new Array[Int](symbol_num)
    val old_b_index_list=new Array[Int](symbol_num)
    val new_f_index_list=new Array[Int](symbol_num)
    val new_b_index_list=new Array[Int](symbol_num)
    val (old_edges:Array[Int],origin_old_f_index_list:Array[Int],origin_old_b_index_list:Array[Int])=e._2._1
    val alllen:Int={
      var tmp=origin_old_b_index_list(symbol_num-1)+1
      for(i<-0 until symbol_num){
        if(new_f_list(i)!=null)
          tmp=tmp+new_f_list(i).size
        if(new_b_list(i)!=null)
          tmp=tmp+new_b_list(i).size
      }
      tmp
    }
    val alle=new Array[Int](alllen)
    var cur_index=0

    var copy_len=origin_old_f_index_list(0)+1
    System.arraycopy(old_edges,0,alle,cur_index,copy_len)
    cur_index += copy_len
    old_f_index_list(0)=cur_index-1

    copy_len=new_f_list(0).size
    System.arraycopy(new_f_list(0).toArray,0,alle,cur_index,copy_len)
    cur_index += copy_len
    new_f_index_list(0)=cur_index-1

    for(i<-1 until symbol_num){
      copy_len=origin_old_f_index_list(i)-origin_old_f_index_list(i-1)
      System.arraycopy(old_edges,origin_old_f_index_list(i-1)+1,alle,cur_index,copy_len)
      cur_index += copy_len
      old_f_index_list(i)=cur_index-1

      copy_len=new_f_list(i).size
      System.arraycopy(new_f_list(i).toArray,0,alle,cur_index,copy_len)
      cur_index += copy_len
      new_f_index_list(i)=cur_index-1
    }

    copy_len=origin_old_b_index_list(0)-origin_old_f_index_list(symbol_num-1)
    System.arraycopy(old_edges,origin_old_f_index_list(symbol_num-1)+1,alle,cur_index,copy_len)
    cur_index += copy_len
    old_b_index_list(0)=cur_index-1

    copy_len=new_b_list(0).size
    System.arraycopy(new_b_list(0).toArray,0,alle,cur_index,copy_len)
    cur_index += copy_len
    new_b_index_list(0)=cur_index-1

    for(i<-1 until symbol_num){
      copy_len=origin_old_b_index_list(i)-origin_old_b_index_list(i-1)
      System.arraycopy(old_edges,origin_old_b_index_list(i-1)+1,alle,cur_index,copy_len)
      cur_index += copy_len
      old_b_index_list(i)=cur_index-1

      copy_len=new_b_list(i).size
      System.arraycopy(new_b_list(i).toArray,0,alle,cur_index,copy_len)
      cur_index += copy_len
      new_b_index_list(i)=cur_index-1
    }
    (flag,(alle,old_f_index_list,new_f_index_list,old_b_index_list,new_b_index_list))
}

  def Union_normal_partition(edges:Iterator[(Int,((Array[Int],Array[Int],Array[Int]),(Array[ArrayBuffer[Int]],
    Array[ArrayBuffer[Int]])))])
  :Iterator[(Int, (Array[Int], Array[Int], Array[Int], Array[Int], Array[Int]))]={
    edges.map(e=>union_normal_elements(e))
  }

  def Split(edges: RDD[(Int, (Iterable[(Array[Int], Array[Int], Array[Int], Array[Int], Array[Int])], Iterable[Array[Array[Int]]]))],
            Split_Threshold:Int,Global_VertexMap:ArrayBuffer[(Int,Int,Int)],curpar:Int)
  : RDD[(Int, (Array[Int], Array[Int], Array[Int], Array[Int], Array[Int]))] = {

    val symbol_num=Property.symbol_num
    val grammar:Array[Array[Int]]=Property.grammar
    val directadd:Map[Int,Int]=Property.directadd
    var cur_nodes_num=Property.nodes_totalnum

    val rdd:RDD[(Int,((Array[Int],Array[Int],Array[Int]),(Array[ArrayBuffer[Int]],Array[ArrayBuffer[Int]])))]=
      edges.mapPartitions(x=>x.map(s=>(s._1,(
        {
          if(s._2._1.isEmpty) {//旧边为空
            val old_f_list=new Array[Int](symbol_num)
            val old_b_list=new Array[Int](symbol_num)
            for(i<-0 until symbol_num){
              old_f_list(i)= -1
              old_b_list(i)= -1
            }
            (Array[Int](),old_f_list,old_b_list)
          }
          else {//旧边不为空
            val old=s._2._1.head
            if (Silence_oldedges(old._2, old._3, old._4, old._5,symbol_num)) {//上一次的新边已合并
              (old._1,old._2,old._4)
            }
            else {
              //上一次的新边尚未合并，因为没有新边插入，简单合并指针即可
              val old_f_index_list = new Array[Int](symbol_num)
              val old_b_index_list = new Array[Int](symbol_num)
              for (i <- 0 until symbol_num) {
                old_f_index_list(i) = old._3(i)
                old_b_index_list(i) = old._5(i)
              }
              (old._1, old_f_index_list, old_b_index_list)
            }
          }
        },{
        if(s._2._2.isEmpty){//新边为空
          val new_f_list = new Array[ArrayBuffer[Int]](symbol_num)
          val new_b_list = new Array[ArrayBuffer[Int]](symbol_num)
          for (i <- 0 until symbol_num) {
            new_f_list(i) = new ArrayBuffer[Int]()
            new_b_list(i) = new ArrayBuffer[Int]()
          }
          (new_f_list,new_b_list)
        }
        else{
            val flag=s._1
            val new_e = s._2._2.head
            val new_e_length = new_e.length
            val new_f_list:Array[ArrayBuffer[Int]]= new Array[ArrayBuffer[Int]](symbol_num)
            val new_b_list:Array[ArrayBuffer[Int]]= new Array[ArrayBuffer[Int]](symbol_num)
            for (i <- 0 until symbol_num) {
              new_f_list(i) = new ArrayBuffer[Int](new_e_length)
              new_b_list(i) = new ArrayBuffer[Int](new_e_length)
            }
            var index = 0
            while (index < new_e_length) {
              if (new_e(index)(1) == flag) {
                new_f_list(new_e(index)(2)).append(new_e(index)(0))
              }
              if (new_e(index)(0) == flag) {
                new_b_list(new_e(index)(2)).append(new_e(index)(1))
              }
              index += 1
            }
            (new_f_list, new_b_list)
        }
      }))),true)

//    println("Before Split Nodes num: "+rdd.count())
    //(Int,p)
    val add_sum=rdd.map(s=>(s._1,{
      var sum:Long=0
      val (old_f_index,old_b_index)=(s._2._1._2,s._2._1._3)
      val (new_f_list,new_b_list)=s._2._2
      for(i<-grammar){
        val f=i(0)
        val b=i(1)
        val target=i(2)
        val old_f_len:Long={
          if (f == 0) old_f_index(0) + 1
          else old_f_index(f) - old_f_index(f - 1)
        }
        val old_b_len:Long={
          if (b == 0) old_b_index(0) - old_f_index(symbol_num-1)
          else old_b_index(b) - old_b_index(b - 1)
        }
        val new_f_len:Long=new_f_list(f).length
        val new_b_len:Long=new_b_list(b).length
        sum={
          val tmp_sum:Long=new_f_len*new_b_len+new_f_len*old_b_len+new_b_len*old_f_len
          if(directadd.contains(target)) sum+2*tmp_sum
          else sum+tmp_sum
        }
      }
      sum
    }))
    val delta_split:Map[Int,Int]={
      add_sum.filter(_._2>Split_Threshold*16).collect().map(s=>(s._1,(Math.sqrt(s._2/Split_Threshold))
        .toInt+1)).toMap
    }
    println("待分裂点与分裂因子: "+ delta_split.size +" ---- "+delta_split.mkString(","))

    if(delta_split.isEmpty==false){//需要分裂
      println("Need Split")
      var final_split:Map[Int,(Int,Int)]=Map()//Int->(p,StartId)
      for(i<-delta_split){
        Global_VertexMap.append((cur_nodes_num,cur_nodes_num+i._2*i._2-1,i._1))
        final_split += i._1 -> (i._2,cur_nodes_num)
        cur_nodes_num=cur_nodes_num+i._2*i._2
      }

      Property.nodes_totalnum=cur_nodes_num

      val split_nodes: RDD[(Int, (Array[Int], Array[Int], Array[Int], Array[Int], Array[Int]))]=
        rdd.filter(s=>delta_split.contains(s._1)).flatMap(s=>{
          val flag=s._1
          val (p:Int,startID:Int)=final_split.getOrElse(s._1,(-1,-1))
          val ((origin_old_e:Array[Int],origin_old_f_index_list:Array[Int],origin_old_b_index_list:Array[Int]),
          (origin_new_f_list:Array[ArrayBuffer[Int]],origin_new_b_list:Array[ArrayBuffer[Int]]))=s._2

          val old_e_list:Array[ArrayBuffer[Int]]=new Array[ArrayBuffer[Int]](p*p)
          val old_f_index_list:Array[Array[Int]]=new Array[Array[Int]](p*p)
          val old_b_index_list:Array[Array[Int]]=new Array[Array[Int]](p*p)
          val new_f_list:Array[Array[ArrayBuffer[Int]]]=new Array[Array[ArrayBuffer[Int]]](p*p)
          val new_b_list:Array[Array[ArrayBuffer[Int]]]=new Array[Array[ArrayBuffer[Int]]](p*p)
          val res:Array[(Int, (Array[Int], Array[Int], Array[Int],Array[Int],Array[Int]))]=new Array(p*p)
          //初始化各数据结构
          for(i<-0 until p*p){
            old_e_list(i)=new ArrayBuffer[Int]()
            old_f_index_list(i)=new Array[Int](symbol_num)
            old_b_index_list(i)=new Array[Int](symbol_num)
            new_f_list(i)=new  Array[ArrayBuffer[Int]](symbol_num)
            new_b_list(i)=new Array[ArrayBuffer[Int]](symbol_num)
            for(j<-0 until symbol_num){
              new_f_list(i)(j)=new ArrayBuffer[Int]()
              new_b_list(i)(j)=new ArrayBuffer[Int]()
            }
          }

          //拷贝
          //旧边
          //f
          for(i<-0 until symbol_num){
            val start= {
              if (i == 0) 0
              else origin_old_f_index_list(i - 1) + 1
            }
            val end=origin_old_f_index_list(i)
            for(index<- start to end){
              val e:Int=origin_old_e(index)
              if(final_split.contains(e)){
                val (pe,startIDe)=final_split.getOrElse(e,(-1,-1))
                for(j<- 0 until pe*pe){
                  val split_e=startIDe+j
                  val mod=split_e%p
                  for(k<-0 until p){
                    val pos= mod + k*p
                    old_e_list(pos).append(e)
                  }
                }
              }
              else{
                val mod=e%p
                for(k<-0 until p){
                  val pos= mod + k*p
                  old_e_list(pos).append(e)
                }
              }
            }
            for(j<- 0 until p*p){
              old_f_index_list(j)(i)=old_e_list(j).size-1
            }
          }
          //b
          for(i<-0 until symbol_num){
            val start= {
              if (i == 0) origin_old_f_index_list(symbol_num-1)+1
              else origin_old_b_index_list(i - 1) + 1
            }
            val end=origin_old_b_index_list(i)
            for(index<- start to end){
              val e=origin_old_e(index)
              if(final_split.contains(e)){
                val (pe,startIDe)=final_split.getOrElse(e,(-1,-1))
                for(j<- 0 until pe*pe){
                  val split_e=startIDe+j
                  val mod=split_e%p
                  for(k<-0 until p){
                    val pos= mod*p + k
                    old_e_list(pos).append(e)
                  }
                }
              }
              else{
                val mod=e%p
                for(k<-0 until p){
                  val pos= mod*p + k
                  old_e_list(pos).append(e)
                }
              }
            }
            for(j<- 0 until p*p){
              old_b_index_list(j)(i)=old_e_list(j).size-1
            }
          }

          //新边
          for(i<-0 until symbol_num){
            //f
            for(e<- origin_new_f_list(i)){
              if(final_split.contains(e)){
                val (pe,startIDe)=final_split.getOrElse(e,(-1,-1))
                for(j<- 0 until pe*pe){
                  val split_e=startIDe+j
                  val mod=split_e%p
                  for(k<-0 until p){
                    val pos= mod + k*p
                    new_f_list(pos)(i).append(e)
                  }
                }
              }
              else{
                val mod:Int= e % p
                for(k<-0 until p){
                  val pos:Int= mod + k*p
                  new_f_list(pos)(i).append(e)
                }
              }
            }
            //b
            for(e:Int<-origin_new_b_list(i)){
              if(final_split.contains(e)){
                val (pe,startIDe)=final_split.getOrElse(e,(-1,-1))
                for(j<- 0 until pe*pe){
                  val split_e=startIDe+j
                  val mod=split_e%p
                  for(k<-0 until p){
                    val pos= mod*p + k
                    new_b_list(pos)(i).append(e)
                  }
                }
              }
              else{
                val mod:Int = e % p
                for(k<-0 until p){
                  val pos= mod*p + k
                  new_b_list(pos)(i).append(e)
                }
              }
            }
          }


          //res赋值
          for(i<-0 until p*p){
            val index=startID+i
            val e_list:Array[Int]=old_e_list(i).toArray
            res(i)=union_normal_elements((index,((e_list,old_f_index_list(i),old_b_index_list(i)),(new_f_list
            (i),new_b_list(i)))))
          }
          res
        })
      println("分裂出的新点个数: "+split_nodes.count())

      val normal_nodes: RDD[(Int, (Array[Int], Array[Int], Array[Int], Array[Int], Array[Int]))]=
        rdd.filter(s=>delta_split.contains(s._1)==false).mapPartitions(x=>x.map(s=>{
          val flag=s._1

          val (old_e:Array[Int],origin_old_f_index_list:Array[Int],origin_old_b_index_list:Array[Int])=s._2._1

          val (origin_new_f_list:Array[ArrayBuffer[Int]],origin_new_b_list:Array[ArrayBuffer[Int]])=s._2._2

          val(new_f_list:Array[ArrayBuffer[Int]],new_b_list:Array[ArrayBuffer[Int]])=(new Array[ArrayBuffer[Int]]
          (symbol_num),new Array[ArrayBuffer[Int]](symbol_num))

          for(i<- 0 until symbol_num){
            new_f_list(i)=new ArrayBuffer[Int]()
            new_b_list(i)=new ArrayBuffer[Int]()
          }
          //新边添加
          for(i<-0 until symbol_num){
            val origin_size=origin_new_f_list(i).length
            for(j<-0 until origin_size){
              val e=origin_new_f_list(i)(j)
              if(final_split.contains(e)){
                new_f_list(i).appendAll(split_ele(flag,final_split.getOrElse(e,(-1,-1)),'f'))
              }
              else new_f_list(i).append(e)
            }
          }

          for(i<-0 until symbol_num){
            val origin_size=origin_new_b_list(i).size
            for(j<-0 until origin_size){
              val e=origin_new_b_list(i)(j)
              if(final_split.contains(e)){
                new_b_list(i).appendAll(split_ele(flag,final_split.getOrElse(e,(-1,-1)),'b'))
              }
              else new_b_list(i).append(e)
            }
          }
          //旧边添加

          val old_f_index_list=new Array[Int](symbol_num)
          val old_b_index_list=new Array[Int](symbol_num)

          val old_e_add:Array[Int]= {
            val old_e_add0=new ArrayBuffer[Int](old_e.length)
            for (i <- 0 until symbol_num) {
              val start = {
                if (i == 0) 0
                else origin_old_f_index_list(i - 1) + 1
              }
              for (j <- start to origin_old_f_index_list(i)) {
                val e = old_e(j)
                if (final_split.contains(e)) {
                  old_e_add0.appendAll(split_ele(flag, final_split.getOrElse(e, (-1, -1)),'f'))
                }
                else old_e_add0.append(e)
              }
              old_f_index_list(i) = old_e_add0.size - 1
            }

            for (i <- 0 until symbol_num) {
              val start = {
                if (i == 0) origin_old_f_index_list(symbol_num - 1) + 1
                else origin_old_b_index_list(i - 1) + 1
              }
              for (j <- start to origin_old_b_index_list(i)) {
                val e = old_e(j)
                if (final_split.contains(e)) {
                  old_e_add0.appendAll(split_ele(flag, final_split.getOrElse(e, (-1, -1)),'b'))
                }
                else old_e_add0.append(e)
              }
              old_b_index_list(i) = old_e_add0.size - 1
            }
            old_e_add0.toArray
          }
          //Union old and new
          union_normal_elements((flag,((old_e_add,old_f_index_list,old_b_index_list),(new_f_list,
            new_b_list))))
        }))

      (split_nodes ++ normal_nodes).repartition(curpar*2)
    }

    else{//不需要分裂，直接合并
      println("Do Not Need Split")
      rdd.mapPartitions(s=>Union_normal_partition(s),true)
    }
  }

  def split_ele(flag:Int, split_info:(Int,Int), e_type:Char):ArrayBuffer[Int]={
    val res=new ArrayBuffer[Int]()
    val (p,startID)=split_info
    val mod=flag%p
    if(e_type == 'f') {
      for (i <- 0 until p) {
        res.append(startID + mod + i * p)
      }
    }
    else{
      for (i <- 0 until p) {
        res.append(startID + mod*p + i )
      }
    }
    res
  }
}

