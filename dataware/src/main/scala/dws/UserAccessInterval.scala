package dws

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DataType, DataTypes, StructType}


//用户访问间隔分析
//guid,first_dt,rng_start,rng_end
object UserAccessInterval {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName(this.getClass.getSimpleName).master("local[*]").getOrCreate()
    val schema = new StructType()
      .add("guid", DataTypes.LongType)
      .add("first_dt", DataTypes.StringType)
      .add("rng_start", DataTypes.StringType)
      .add("rng_end", DataTypes.StringType)
    val frame = spark.read.schema(schema).csv("file:///C:\\Users\\hp\\IdeaProjects\\TiTan\\dataware\\src\\main\\resources\\UserAccessInterval.csv")
    frame.createTempView("tmp")
    /*1.过滤出rng_end时间与今天相比小于30的数据
    2.把rng_end为9999-12-31的日期改成2020-03-14
    3.如果first_dt与今天相比大于30天则更改为今天的日期-30,否则不变*/
    val frame1 = spark.sql(
      """
        |select
        |  guid,
        |  if(datediff('2020-03-14',rng_start)>=30,date_sub('2020-03-14',30),rng_start) as rng_start,
        |  if(rng_end = '9999-12-31','2020-03-14',rng_end) as rng_end
        |from
        |  tmp
        |where datediff('2020-03-14',rng_end) <=30
        |""".stripMargin)

    //统计登陆间隔
    //按用户来分组
    val guidrdd = frame1.rdd.map(row => {
      val guid = row.getAs[Long]("guid")
      val rng_start = row.getAs[String]("rng_start")
      val rng_end = row.getAs[String]("rng_end")
      (guid, (rng_start, rng_end))
    }).groupByKey()

    val value = guidrdd.flatMap(data => {
      val guid = data._1
      val sorted = data._2.toList.sortBy(_._2)
      //统计间隔为0的次数
      val list: List[(Long, Int, Int)] = for (elem <- sorted) yield (guid, 0, datediff(elem._1, elem._2))

      //统计间隔为N的次数(要排序),用后一个的rng_start- 前一个的 rng_end
      val list1 = for (i <- 0 until sorted.length - 1) yield (guid, datediff(sorted(i)._2, sorted(i + 1)._1), 1)
      list ++ list1
    })
    import spark.implicits._
    import org.apache.spark.sql.functions._
    value.toDF("guid", "interval", "times")
      .groupBy("guid", "interval").agg(sum("times") as "times")
      .show(100, false)
  }

  //时间相减
  def datediff(date1: String, date2: String): Int = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val s1: Date = sdf.parse(date2)
    val s2: Date = sdf.parse(date1)
    ((s1.getTime - s2.getTime) / (24 * 60 * 60 * 1000)).toInt
  }
}
