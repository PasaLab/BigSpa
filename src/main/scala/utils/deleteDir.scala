package utils

/**
  * Created by cycy on 2018/1/14.
  */
import java.io._
import java.net.URI

import org.apache.hadoop.fs.Path

object deleteDir {
  def deletedir(islocal:Boolean,master:String,pathstr:String):Unit={
    if(islocal){ //delete local file
    val path=new File(pathstr)
      if (!path.exists()) {
      }
      else if (path.isFile()) {
        path.delete()
      }
      else {
        val files = path.listFiles()
        for (i <- path.listFiles()) {
          deletedir(islocal,master,i.getAbsolutePath)
        }
        path.delete()
      }
    }
    else{//delete hdfs file
    val path=new Path(pathstr)
      val hdfs=org.apache.hadoop.fs.FileSystem.get(
        new URI(master),new org.apache.hadoop.conf.Configuration())
      //删除输出目录
      if(hdfs.exists(path)){
        hdfs.delete(path)
      }
    }
  }



}
