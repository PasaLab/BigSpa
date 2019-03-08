//package ONLINE
//
//import java.util
//
//import it.unimi.dsi.fastutil.ints.IntOpenHashSet
//import it.unimi.dsi.fastutil.longs.LongOpenHashSet
//import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
//import org.apache.spark.rdd.RDD
//import utils_ONLINE.{BIgSpa_OP, Param_Online}
//
//import scala.collection.mutable.ArrayBuffer
//import scala.io.Source
//
///**
//  * Created by cycy on 2019/1/21.
//  */
//object DeltaSpa_20190121 {
//
//  val in=0
//  val out=1
//
//  def deltaQuery(sc:SparkContext,
//                 deltaEdges:Array[(Int,Array[Array[ArrayBuffer[Int]]])], graph:RDD[(Int,Array[Array[ArrayBuffer[Int]]])],
//                 hop:Int,grammar:Array[Array[Int]],symbol_num:Int,par:Int,reverse_symbol_Map:Map[Int,String])
//  :RDD[(Int,Array[Array[ArrayBuffer[Int]]])]={
//    /**
//      * 大批量版
//      */
//    val oldEdgesPartitioner = graph.partitioner.getOrElse(new HashPartitioner(par))
//    val deltaEdgesRDD:RDD[(Int,Array[Array[ArrayBuffer[Int]]])]=sc.parallelize(deltaEdges).partitionBy(oldEdgesPartitioner)
//
//    val new_graph=graph.cogroup(deltaEdgesRDD,oldEdgesPartitioner).mapPartitions(p=>p.map(v=>(v._1,{
//      val old_e0=v._2._1
//      val new_e0=v._2._2
//      if(old_e0.isEmpty) new_e0.head
//      else if(new_e0.isEmpty) old_e0.head
//      else{
//        val old_e=old_e0.head
//        val new_e=new_e0.head
//        for(label<-0 until symbol_num){
//          old_e(label)(in).appendAll(new_e(label)(in))
//          old_e(label)(out).appendAll(new_e(label)(out))
//        }
//        old_e
//      }
//    })))
//
//    val res=QueryNHop(sc,new_graph,hop,deltaEdges,grammar,symbol_num,reverse_symbol_Map)
//    println("Query res: ")
//    for(node<- res){
//      println(s"node "+node._1+"'s info: ")
//      for(i<-0 until symbol_num){
//        if(node._2._1(i).size>0||node._2._2(i).size>0){
//          println("label - "+reverse_symbol_Map(i)+" : "+node._2._1(i).map(s=>s+" -> "+node._1).mkString("\t")+node._2
//            ._2(i).map(s=>node._1+" -> "+s).mkString("\t"))
//        }
//      }
//    }
//    new_graph
//  }
//
//  def checkExistsAndUpdateChecker(src:Int,target:Int,label:Int,checker:LongOpenHashSet): Boolean ={
//    val l=utils_ONLINE.BigSpa_OP_java.edge2long(src,target,label)
//    if(checker.contains(l)) true
//    else{
//      checker.add(l)
//      false
//    }
//  }
//  def checkExistsAndUpdateChecker_Nodes(node:Int,checker_nodes:IntOpenHashSet): Boolean ={
//    if(checker_nodes.contains(node)) true
//    else{
//      checker_nodes.add(node)
//      false
//    }
//  }
//
//  def QueryNHop(sc:SparkContext,graph:RDD[(Int,Array[Array[ArrayBuffer[Int]]])],hop:Int,
//                deltaEdges:Array[(Int,Array[Array[ArrayBuffer[Int]]])],grammar:Array[Array[Int]],symbol_num:Int,
//                reverse_symbol_Map:Map[Int,String])
//  :Array[(Int,(Array[ArrayBuffer[Int]],Array[ArrayBuffer[Int]]))] ={
//    val toQuery:Set[Int]=deltaEdges.map(es=>es._1).toSet
//    val checker=new LongOpenHashSet()
//    val checker_nodes=new IntOpenHashSet()
//
//    println("deltaedges length: "+deltaEdges.length)
////    val v_adj:Array[(Int,Array[Array[ArrayBuffer[Int]]])]=deltaEdges.map(v=>(v._1,{
////      val node=v._1
////      /**
////        * 初始化 adj
////        */
////      val adj = new Array[Array[ArrayBuffer[Int]]](symbol_num)
////      for (label <- 0 until symbol_num) {
////        adj(label) = new Array[ArrayBuffer[Int]](2)
////        adj(label)(in) = new ArrayBuffer[Int]()
////        adj(label)(out) = new ArrayBuffer[Int]()
////        adj(label)(in).appendAll(v._2(label)(in))
////        adj(label)(out).appendAll(v._2(label)(out))
////        adj(label)(in).map(x=>checker.add(utils.BigSpa_OP_java.edge2long(x,node,label)))
////        adj(label)(out).map(x=>checker.add(utils.BigSpa_OP_java.edge2long(node,x,label)))
////      }
////      adj
////    }))
////
////    var tmp_v_adj=deltaEdges
////    for(iteration <- 0 to hop) {
////      val toQuery: Array[Int] = tmp_v_adj.flatMap(s => {
////        val ab: ArrayBuffer[Int] = ArrayBuffer()
////        for (a <- s._2) {
////          for (b <- a) {
////            ab.appendAll(b)
////          }
////        }
////        ab
////      }).distinct
////      val e0: Map[Int, Array[Array[ArrayBuffer[Int]]]] = graph.filter(s => toQuery.contains(s._1)).collect().toMap
////      tmp_v_adj = tmp_v_adj.map(m => (m._1, {
////        /**
////          * 初始化 adj
////          */
////        val adj = new Array[Array[ArrayBuffer[Int]]](symbol_num)
////        for (label <- 0 until symbol_num) {
////          adj(label) = new Array[ArrayBuffer[Int]](2)
////          adj(label)(in) = new ArrayBuffer[Int]()
////          adj(label)(out) = new ArrayBuffer[Int]()
////        }
////
////        /**
////          * 寻找前后的下一跳
////          */
////        for (label <- 0 until symbol_num) {
////          val (label_in, label_out) = grammar_path.get(label).get
////          if (m._2(label)(in).length > 0 && label_in != -1) {
////            for (mid <- m._2(label)(in)) {
////              for (from <- e0.get(mid).get(label_in)(in)) {
////                if (checkExistsAndUpdateChecker(from, m._1, label_in, checker) == false) {
////                  adj(label_in)(in).append(from)
////                }
////              }
////            }
////          }
////          if (m._2(label)(out).length > 0 && label_out != -1) {
////            for (mid <- m._2(label)(out)) {
////              for (back <- e0.get(mid).get(label_out)(out)) {
////                if (checkExistsAndUpdateChecker(m._1, back, label_out, checker) == false) {
////                  adj(label_out)(out).append(back)
////                }
////              }
////            }
////          }
////        }
////        /**
////          * 更新 v_adj,保存每一次新增的边，供最后输出
////          */
////        for (i <- 0 to v_adj.length) {
////          for (label <- 0 until symbol_num) {
////            v_adj(i)._2(label)(in).appendAll(tmp_v_adj(i)._2(label)(in))
////            v_adj(i)._2(label)(out).appendAll(tmp_v_adj(i)._2(label)(out))
////          }
////        }
////        /**
////          * 返回更新后的可达点
////          */
////        adj
////      }))
////    }
//    /**
//      * 初始化checker
//      */
//      deltaEdges.map(es=>{
//        val flag=es._1
//        for(i<-0 until symbol_num){
//          es._2(i)(in).map(from=>checker.add(utils_ONLINE.BigSpa_OP_java.edge2long(from,flag,i)))
//          es._2(i)(out).map(back=>checker.add(utils_ONLINE.BigSpa_OP_java.edge2long(flag,back,i)))
//        }
//      })
//
//    val v_adj:ArrayBuffer[(Int,Array[Array[ArrayBuffer[Int]]])]=new ArrayBuffer[(Int, Array[Array[ArrayBuffer[Int]]])
//      ]()
//    /**
//      * 寻找N 跳内可达的点
//      */
//    var tmp_new_edges:Array[(Int,Array[Array[ArrayBuffer[Int]]])]=deltaEdges
//    for(i<-0 until hop){
//      val delta_nodes=tmp_new_edges.flatMap(s=>{
//        val nodes=ArrayBuffer[Int]()
//        for(i<- 0 until symbol_num){
//          nodes.appendAll(s._2(i)(in))
//          nodes.appendAll(s._2(i)(out))
//        }
//        nodes
//      }).distinct.filter(n=>checkExistsAndUpdateChecker_Nodes(n,checker_nodes)==false)
//      println("deltanodes: "+delta_nodes.mkString(","))
//      val old_partitioner=graph.partitioner
//      tmp_new_edges=graph.filter(v=>delta_nodes.contains(v._1)).collect()
//      v_adj.appendAll(tmp_new_edges)
//    }
//    println("v_adj:")
//    for(node<- v_adj){
//      println(s"node "+node._1+"'s info: ")
//      for(i<-0 until symbol_num){
//        if(node._2(i)(in).size>0||node._2(i)(out).size>0){
//          println("label - "+reverse_symbol_Map(i)+" : "+node._2(i)(in).map(s=>s+" -> "+node._1).mkString("\t")+node._2
//          (i)(out).map(s=>node._1+" -> "+s).mkString("\t"))
//        }
//      }
//    }
//    /**
//      * 计算N跳内闭包
//      * 1 将v_adj改造为 v old_f old_b new_f new_b形式
//      * 2 根据grammar计算 新边
//      *   2.1 checker 过滤
//      *   2.2 添加new_edges
//      * 3 new_edges整理为v new_f new_b
//      * 4 合并new_edges 和v_adj
//      *   4.1 v_adj new_f->old_f,new_b->old_b
//      *   4.2 new_edges.new_f->new_f,new_edges.new_b->new_b
//      */
//    var v_adj_compute:Map[Int,(Array[ArrayBuffer[Int]],Array[ArrayBuffer[Int]],Array[ArrayBuffer[Int]],
//      Array[ArrayBuffer[Int]])]=v_adj.map(v_a=>(v_a._1,{
//      val adj:(Array[ArrayBuffer[Int]],Array[ArrayBuffer[Int]],Array[ArrayBuffer[Int]], Array[ArrayBuffer[Int]])=
//        (new Array(symbol_num),new Array(symbol_num),new Array(symbol_num),new Array(symbol_num))
//      for(i<-0 until symbol_num){
//        adj._1(i)=ArrayBuffer[Int]()
//        adj._2(i)=ArrayBuffer[Int]()
//        adj._3(i)=v_a._2(i)(in)
//        adj._4(i)=v_a._2(i)(out)
//      }
//      adj
//    })).toMap
//
//    var continue=true
//    while(continue){
//      val e0=checker.size()
//      val new_edges:ArrayBuffer[Array[Int]]=ArrayBuffer()
//      /**
//        * 计算新边
//        */
//      v_adj_compute.map(v_a=>{
//        for(g<-grammar){
//          for(from<-v_a._2._3(g(0))){
//            /**
//              * new_f new_b
//              */
//            for(back<-v_a._2._4(g(1))){
//              if(checkExistsAndUpdateChecker(from,back,g(2),checker)==false){//新边
//                new_edges.append(Array(from,back,g(2)))
//                println(s"new_f,new_b: "+from,back,g(2))
//              }
//            }
//            /**
//              * new_f old_b
//              */
//            for(back<-v_a._2._2(g(1))){
//              if(checkExistsAndUpdateChecker(from,back,g(2),checker)==false){//新边
//                new_edges.append(Array(from,back,g(2)))
//                println(s"new_f,old_b: "+from,back,g(2))
//              }
//            }
//          }
//          /**
//            * old_f new_b
//            */
//          for(from<-v_a._2._1(g(0))){
//            for(back<-v_a._2._4(g(1))){
//              if(checkExistsAndUpdateChecker(from,back,g(2),checker)==false){//新边
//                new_edges.append(Array(from,back,g(2)))
//                println(s"old_f,new_b: "+from,back,g(2))
//              }
//            }
//          }
//
//        }
//      })
//
//      /**
//        * 更新新边
//        */
//        val group_new_edges:Map[Int,(Array[ArrayBuffer[Int]],Array[ArrayBuffer[Int]])]=new_edges.flatMap(s=>{
//        Array((s(0),s),(s(1),s))
//      }).groupBy(_._1)
//          .map(v=>(v._1,{
//        /**
//          * 初始化 adj
//          */
//        val adj =(new Array[ArrayBuffer[Int]](symbol_num),new Array[ArrayBuffer[Int]](symbol_num))
//        for (i <- 0 until symbol_num) {
//          adj._1(i)=ArrayBuffer()
//          adj._2(i)=ArrayBuffer()
//        }
//        /**
//          * 分配各label
//          */
//        val flag=v._1
//        for(e0<-v._2) {
//          val e=e0._2
//          if(e(0)==flag){
//            adj._2(e(2)).append(e(1))
//          }
//          if(e(1)==flag){
//            adj._1(e(2)).append(e(0))
//          }
//        }
//        adj
//      }))
//
//      v_adj_compute=v_adj_compute.map(v_a=>(v_a._1,{
//        for(i<-0 until symbol_num){
//          v_a._2._1(i).appendAll(v_a._2._3(i))
//          v_a._2._2(i).appendAll(v_a._2._4(i))
//        }
//        val tmp:(Array[ArrayBuffer[Int]],Array[ArrayBuffer[Int]])={
//        if(group_new_edges.contains(v_a._1))
//          group_new_edges.get(v_a._1).get
//        else (Array.range(0,symbol_num).map(s=>new ArrayBuffer[Int]()),Array.range(0,symbol_num).map(s=>new
//            ArrayBuffer[Int]()))
//        }
//        (v_a._2._1,v_a._2._2,tmp._1,tmp._2)
//      }))
//
//      println("turn")
//      println("!!!!!!!!!!!!!!!!!!!!!!!!!!!")
//      println("print v_adj_compute")
//      for(node<- v_adj_compute){
//        println(s"node "+node._1+"'s info: ")
//        for(i<-0 until symbol_num){
//          if(node._2._1(i).size>0||node._2._2(i).size>0){
//            println("label - "+reverse_symbol_Map(i)+" : "+node._2._1(i).map(s=>s+" -> "+node._1).mkString("\t")
//              +" || "+node._2._2(i).map(s=>node._1+" -> "+s).mkString("\t")
//            +" || "+node._2._3(i).map(s=>node._1+" -> "+s).mkString("\t")
//            +" || "+node._2._4(i).map(s=>node._1+" -> "+s).mkString("\t"))
//          }
//        }
//      }
//      println("!!!!!!!!!!!!!!!!!!!!!!!!!!!")
//      if(checker.size()==e0) continue=false
//    }
//
//    v_adj_compute.filter(s=>toQuery.contains(s._1)).map(s=>(s._1,(s._2._1,s._2._2))).toArray
//  }
//
//  def main(args: Array[String]): Unit = {
//    Param_Online.makeParams(args)
//    val conf = new SparkConf()
//    if (Param_Online.islocal) {
//      //test location can be adjusted or not
//      conf.setAppName("Graspan")
//      System.setProperty("hadoop.home.dir", "F:/hadoop-2.6.0/")
//      conf.setMaster("local")
//    }
//
//    val sc = new SparkContext(conf)
//    sc.setLogLevel("Error")
//    sc.setCheckpointDir(Param_Online.checkpoint_output)
//
//    /**
//      * Grammar相关设置
//      */
//    val grammar_origin = sc.textFile(Param_Online.input_grammar).filter(s=> !s.trim.equals("")).map(s => s.split("\\s+").map(_
//      .trim))
//      .collect().toList
//    val (symbol_Map, symbol_num, symbol_num_bitsize, loop, directadd, grammar) = BIgSpa_OP.processGrammar(grammar_origin,
//      Param_Online.input_grammar)
//    println("------------Grammar INFO--------------------------------------------")
//    println("input grammar:      \t" + Param_Online.input_grammar.split("/").last)
//    println("symbol_num:         \t" + symbol_num)
//    println("symbol_num_bitsize: \t" + symbol_num_bitsize)
//    println("symbol_Map:         \t")
//    symbol_Map.foreach(s => println("                    \t" + s._2 + "\t->\t" + s._1))
//    println
//    println("loop:               \t")
//    loop.foreach(s => println("                    \t" + s))
//    println
//    println("directadd:          \t")
//    directadd.foreach(s => println("                    \t" + s._1 + "\t->\t" + s._2))
//    println
//    println("grammar_clean:      \t")
//    grammar.foreach(s => println("                    \t" + s(0) + "\t+\t" + s(1) + "\t->\t" + s(2)))
//    println("---------------------------------------------------------------------")
//    println
//
//    val reverse_symbol_Map:Map[Int,String]=symbol_Map.map(s=>(s._2,s._1))
//
//    val lines=Source.fromFile(Param_Online.input_graph).getLines()
//    var graph:RDD[(Int,Array[Array[ArrayBuffer[Int]]])]=sc.parallelize(Array[(Int,Array[Array[ArrayBuffer[Int]]])]())
//    for(line<- lines){
//      println(line.split(",").mkString("\n"))
//      val edges0:Array[Array[Int]]=line.split(",").map(s=>s.split("\\s+").map(_.toInt))
//      var toQuery:ArrayBuffer[Int]=ArrayBuffer()
//      edges0.flatMap(s=>Array(s(0),s(1))).map(s=>toQuery.append(s))
//      toQuery=toQuery.distinct
//      val loops_edges:ArrayBuffer[Array[Int]]=toQuery.flatMap(s=>loop.map(x=>Array(s,s,x)))
//      loops_edges.appendAll(edges0)
//      val directadd_edges=loops_edges.filter(s=>directadd.contains(s(2))).map(s=>Array(s(0),s(1),directadd.getOrElse
//      (s(2),-1)))
//      loops_edges.appendAll(directadd_edges)
//      println("after add loops and directadd : all edges - ")
//      for(e<- loops_edges)
//        println(e.mkString(","))
//      val deltaEdges:Array[(Int,Array[Array[ArrayBuffer[Int]]])]=loops_edges.flatMap(s=>{
//        Array((s(0),s),(s(1),s))
//      }).groupBy(_._1).map(v=>(v._1,{
//        /**
//          * 初始化 adj
//          */
//        val adj = new Array[Array[ArrayBuffer[Int]]](symbol_num)
//        for (label <- 0 until symbol_num) {
//          adj(label) = new Array[ArrayBuffer[Int]](2)
//          adj(label)(in) = new ArrayBuffer[Int]()
//          adj(label)(out) = new ArrayBuffer[Int]()
//        }
//        /**
//          * 分配各label
//          */
//        val flag=v._1
//        for(e0<-v._2) {
//          val e=e0._2
//          if(e(0)==flag){
//            adj(e(2))(out).append(e(1))
//          }
//          if(e(1)==flag){
//            adj(e(2))(in).append(e(0))
//          }
//        }
//        adj
//      })).toArray
////      println("deltaEdges:")
////      for(node<-deltaEdges) {
////        println(s"node " + node._1 + "'s info: ")
////        for (i <- 0 until symbol_num) {
////          if (node._2(i)(in).size > 0 || node._2(i)(out).size > 0) {
////            println("label - " + reverse_symbol_Map(i) + " : " + node._2(i)(in).map(s => s + " -> " + node._1)
////              .mkString("\t")
////              + node._2(i)(out).map(s => node._1 + " -> " + s).mkString("\t"))
////          }
////        }
////      }
//
//      graph=deltaQuery(sc,deltaEdges,graph,Param_Online.hop,grammar,symbol_num,Param_Online.defaultpar, reverse_symbol_Map)
//      println("graph:")
//      for(node<-graph.collect()){
//        println(s"node "+node._1+"'s info: ")
//        for(i<-0 until symbol_num){
//          if(node._2(i)(in).size>0||node._2(i)(out).size>0){
//            println("label - "+reverse_symbol_Map(i)+" : "+node._2(i)(in).map(s=>s+" -> "+node._1).mkString("\t")
//              +node._2(i)(out).map(s=>node._1+" -> "+s).mkString("\t"))
//          }
//        }
//      }
//    }
//  }
//}
