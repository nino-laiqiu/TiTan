package data

import java.net.URI

import org.apache.commons.io.output.ByteArrayOutputStream
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, Path}
import org.apache.hadoop.io.IOUtils

//ip的工具类
object GeodictUtil {

  def Geodict()= {
    val fs: FileSystem = FileSystem.get(new URI("hdfs://linux03:8020"), new Configuration(), "root")
    val fSDataInputStream: FSDataInputStream = fs.open(new Path("/mydata/ip2region.db"))
    val stream: ByteArrayOutputStream = new ByteArrayOutputStream()
    IOUtils.copyBytes(fSDataInputStream, stream, 1024)
    stream.toByteArray
  }
}
