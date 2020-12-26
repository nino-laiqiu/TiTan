import java.io.{BufferedReader, File, FileInputStream, InputStreamReader}
import java.net.URI
import java.nio.file.{Files, Paths}

import com.sun.tools.javac.util.ListBuffer
import org.apache.commons.io.output.ByteArrayOutputStream
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, Path}
import org.lionsoul.ip2region.{DbConfig, DbSearcher}
import org.apache.hadoop.io.IOUtils

import scala.io.Source;


//测试geodict
object GeodictTest {

  def main(args: Array[String]): Unit = {
    // readHdfs()
    //readlocality()
    readHdfs_List()

  }

  def readLocality() = {
    //方案一,读取本地文件,
    val filePath = "C:\\Users\\hp\\IdeaProjects\\TiTan\\dataware\\src\\main\\resources\\ip2region.db";
    val ip2regionBytes = Files.readAllBytes(Paths.get(filePath))
    val config = new DbConfig
    // 构造搜索器,dbFile是ip地址库字典文件所在路径
    val searcher = new DbSearcher(config, ip2regionBytes)
    // 使用搜索器,调用查找算法获取地理位置信息
    val block = searcher.memorySearch("222.64.158.53")
    println(block.getRegion)
  }

  def readHdfs() = {
    //方案二:读取hdfs
    val fs: FileSystem = FileSystem.get(new URI("hdfs://linux03:8020"), new Configuration(), "root")
    val fSDataInputStream: FSDataInputStream = fs.open(new Path("/mydata/ip2region.db"))
    val stream = new ByteArrayOutputStream();
    /*   var bytes = new Array[Byte](1024);
         var len = -1;
       while ((len = fSDataInputStream.read(bytes)) != -1) {
           stream.write(bytes, 0, len)
         }
         val ip2regionBytes = stream.toByteArray*/
    IOUtils.copyBytes(fSDataInputStream, stream, 1024)
    val config = new DbConfig
    val searcher = new DbSearcher(config, stream.toByteArray)
    val block = searcher.memorySearch("110.165.130.36")
    println(block.getRegion)
  }

  //  把hdfs中的数据读取到集合中
  def readHdfs_List(): Unit = {
    val fs = FileSystem.newInstance(new URI("hdfs://linux03:8020"), new Configuration(), "root")
    val fSDataInputStream = fs.open(new Path("/mydata/words.txt"))
    val reader = new BufferedReader(new InputStreamReader(fSDataInputStream))
    var list = new ListBuffer[String]()
    var line: String = ""
    // line = reader.readLine() 是没有返回值的,考虑使用一个flag
    /* while ((line = reader.readLine()) != null) {
       //把读取到的字符串数据存储到集合中
       list + line
     }*/
    val str = reader.readLine()
    println(str)
    var flag = true
    while (flag) {
      line = reader.readLine()
      println(line)
     if (line  !=  null){
        list.append(line)
      }
     else {
        flag = false
      }
    }
    list.forEach(println)
    //list.forEach(println)
    fSDataInputStream.close()
    fs.close()
  }
}
