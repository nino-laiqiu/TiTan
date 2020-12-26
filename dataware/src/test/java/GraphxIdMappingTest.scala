import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkConf
import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}

//图计算api的测试
object GraphxIdMappingTest {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().config(new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")).getOrCreate()
    // val rdd = spark.sparkContext.textFile("file:///C:\\Users\\hp\\IdeaProjects\\TiTan\\dataware\\src\\main\\resources\\graphxdemo.txt")
    import spark.implicits._
    val ds: Dataset[String] = spark.read.textFile("file:///C:\\Users\\hp\\IdeaProjects\\TiTan\\dataware\\src\\main\\resources\\graphxdemo.txt")
    val rdd = ds.rdd
    //构建点,点的唯一标识类型是Long类型
    val dot: RDD[(Long, String)] = rdd.flatMap(line => {
      // TODO 使用原生的hashcode在数据量不多的情况下可以使用,但是在大数据量会发生大量hash冲突,采用选用谷歌
      // TODO 开发的 Guava 库中的 Hashing 工具。利用该工具，可以通过 MD5 哈希算法为每个主题生
      // TODO 成一个 64 位的唯一标识符
      for (elem <- line.split(",") if (StringUtils.isNotBlank(elem))) yield (elem.hashCode.toLong, elem)
    })
    //构建边,注意是否数组越界
    val side: RDD[Edge[String]] = rdd.flatMap(lines => {
      val line = lines.split(",")
      for (i <- 0 to line.length - 2 if ((StringUtils.isNotBlank(line(0))))) yield Edge(line(i).hashCode.toLong, line(i + 1).hashCode.toLong, "")
    })
    //构建图
    val graph = Graph(dot, side)
    //连同子图算法
    val graphDots: VertexRDD[VertexId] = graph.connectedComponents().vertices
    val grapBroad = spark.sparkContext.broadcast(graphDots.collectAsMap())

    rdd.map(lines => {
      val line = lines.split(",").filter(StringUtils.isNotBlank(_))
      val onlyId = grapBroad.value(line(0).hashCode.toLong)
      onlyId + "" + lines
    }).toDF().show(10, truncate = false)
  }
}
