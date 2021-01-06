package dws

import org.apache.spark.sql.SparkSession

//用户活跃天数统计
//guid,first_dt,rng_start,rng_end
object UserActivityAnalysis {
  //注意传入日期的格式:'2020-06-03'
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName(this.getClass.getSimpleName).getOrCreate()
    val dataFrame = spark.read.option("header",true.toString).csv("file:///C:\\Users\\hp\\IdeaProjects\\TiTan\\dataware\\src\\main\\resources\\Useractivityanalysis.csv")
      .where("rng_end = '9999-12-31'")
      .selectExpr("cast(guid as int) ", s"datediff(${args(0)},rng_start) as days")

      import  spark.implicits._
      // 循环获取天数
    dataFrame.show(100,false)
    val value = dataFrame.rdd.flatMap(row => {
      val guid = row.getAs[Int]("guid")
      val days = row.getAs[Int]("days")
      for (i <- 1 to days + 1) yield (guid, i)
    })

    value.toDF("guid","days").createTempView("UserActivity")

    spark.sql(
      s"""
        |select
        |${args(0)} as dt,
        |days ,--天数
        |count(1) ,-- 人数
        |collect_set(guid) -- 人
        |from
        |UserActivity
        |group by days
        |order by days
        |""".stripMargin).show(100,false)
  }
}
