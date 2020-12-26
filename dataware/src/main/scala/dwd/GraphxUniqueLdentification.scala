package dwd

import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkConf
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}

object GraphxUniqueLdentification {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().config(new SparkConf())
      .appName(this.getClass.getSimpleName)
      .enableHiveSupport()
      .master("local[*]")
      .getOrCreate()
    import spark.implicits._
    import org.apache.spark.sql.functions._
    val frame = spark.read.table("db_demo1.app_event_log")

    val dataFrame = frame.select("account", "deviceId")
    //暂时的一个用户标识
    val temporaryOne: RDD[Array[String]] = dataFrame.rdd.map(row => {
      val account = row.getAs[String](0)
      val deviceId = row.getAs[String](1)
      Array(account, deviceId).filter(StringUtils.isNotBlank(_))
    })
    //println(temporaryOne.collect().length)

    // TODO 构造图的点和边

    val today_dot: RDD[(Long, String)] = temporaryOne.flatMap(arr => {
      for (element <- arr) yield (element.hashCode.toLong, element)
    })

    val today_side: RDD[Edge[String]] = temporaryOne.flatMap(arr => {
      for (i <- 0 to arr.length - 2; j <- i + 1 until arr.length) yield Edge(arr(i).hashCode.toLong, arr(j).hashCode.toLong, "")
    }).map((_, 1))
      .reduceByKey(_ + _)
      .filter(_._2 > 2)
      .map(_._1)

    // TODO 这里是把第一次的日志获取的用户标签写入到hive中
    //firstOneGuid(today_dot,today_side)


    // todo  生成今天的 DUID && 用户标签写入到hive中
    //today_Guid(today_dot,today_side)


    // todo 将今日的图计算结果与昨天的结果进行比对,如果最小的 guid 出现在今天了,则替换为昨天的guid

    //分别读取hive表中的用户标签字段
    val last_identifying: collection.Map[VertexId, VertexId] = spark.read.table("dw.graphData").select("identifying_hash", "guid").where("dy='2020-12-16'").rdd
      .map(row => {
        (row.getAs[VertexId](0), row.getAs[VertexId](1))
      })
      .collectAsMap()

    //把昨天的标签数据广播出去 把今天的数据按照guid按照分组
    val last_identifying_broadcast = spark.sparkContext.broadcast(last_identifying)

    val today_identifying: RDD[(VertexId, VertexId)] = spark.read.table("dw.graphData").select("identifying_hash", "guid").where("dy='2020-12-17'").rdd
      .map(row => {
        (row.getAs[VertexId]("guid"), row.getAs[VertexId]("identifying_hash"))
      })
      .groupByKey()
      .mapPartitions(iter => {
        //获取映射字典
        val idmpMap = last_identifying_broadcast.value
        iter.map(tp => {
          var guid = tp._1
          val identifying_hash = tp._2
          var findmin_guid = false
          for (elem <- identifying_hash if !findmin_guid) {
            //如果在今日存在 昨天的那个最小的guid 那么就把今天的guid替换为昨天的那个
            val maybeId: Option[VertexId] = idmpMap.get(elem)
            if (maybeId.isDefined) {
              guid = maybeId.get
              findmin_guid = true
            }
          }
          (guid, identifying_hash)
        })
      }).flatMap(tp => {
      //扁平化
      val guid = tp._1
      val identifying_hash = tp._2
      for (elem <- identifying_hash) yield (elem, guid)
    })


    //today_identifying.toDF("identifying_hash", "guid").show(20)
    //把数据写入到hive表中,覆盖今天的标签表
    today_identifying.toDF("identifying_hash", "guid").createTempView("graph")

    spark.sql(
      """
        |insert overwrite table  dw.graphData   partition(dy='2020-12-17')
        |select
        |identifying_hash,guid
        |from
        |graph
        |""".stripMargin)


    def today_Guid(today_dot: RDD[(Long, String)], today_side: RDD[Edge[String]]) {
      // TODO  读取上一日的生成的标签字段
      val oneday_identifying: RDD[Row] = spark.read.table("dw.graphData").select("identifying_hash", "guid").where("dy='2020-12-16'").rdd

      val last_dot: RDD[(VertexId, String)] = oneday_identifying.map(row => {
        val identifying_hash = row.getAs[VertexId](0)
        (identifying_hash, "")
      })

      val last_side: RDD[Edge[String]] = oneday_identifying.map(row => {
        val scr: VertexId = row.getAs[VertexId]("identifying_hash")
        val dst = row.getAs[VertexId]("guid")
        Edge(scr, dst, "")

      })
      // 构建图
      val graph = Graph(today_dot.union(last_dot), today_side.union(last_side))
      graph.connectedComponents().vertices.toDF("identifying_hash", "guid").createTempView("graph")

      spark.sql(
        """
          |insert  into  table  dw.graphData   partition(dy='2020-12-17')
          |select
          |identifying_hash,guid
          |from
          |graph
          |""".stripMargin)
    }


    def firstOneGuid(dot: RDD[(Long, String)], side: RDD[Edge[String]]) {
      val graph = Graph(dot, side)
      val conngraph = graph.connectedComponents().vertices
      // todo 此处的guid是标识中的最小的值,我们可以把这个guid替换为一个生成的唯一的UUID来作为一个guid

      val graphDataFrame = conngraph.toDF("identifying_hash", "guid")
      graphDataFrame.createTempView("graph")

      // TODO 把用户唯一标识库写入到hive表中
      spark.sql(
        """
          |insert  into  table  dw.graphData   partition(dy='2020-12-16')
          |select
          |identifying_hash,guid
          |from
          |graph
          |""".stripMargin).show()
    }
  }
}
