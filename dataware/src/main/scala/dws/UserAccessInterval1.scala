package dws

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DataTypes, StructType}


//用户访问间隔分析
//guid,first_dt,rng_start,rng_end
object UserAccessInterval1 {
  def main(args: Array[String]): Unit = {
    //UserAccessInterval是spark实现,UserAccessInterval1是SQL实现

    val spark = SparkSession.builder().appName(this.getClass.getSimpleName).master("local[*]").getOrCreate()
    val schema = new StructType()
      .add("guid", DataTypes.LongType)
      .add("first_dt", DataTypes.StringType)
      .add("rng_start", DataTypes.StringType)
      .add("rng_end", DataTypes.StringType)
    val frame = spark.read.option("header", true.toString).schema(schema).csv("file:///C:\\Users\\hp\\IdeaProjects\\TiTan\\dataware\\src\\main\\resources\\UserAccessInterval.csv")
    frame.createTempView("usertable")
    spark.sql(
      """
        |with x as (
        |select
        |guid,
        |if(datediff('2020-03-14',rng_start) >= 30,date_sub('2020-03-14',30),rng_start) as rng_start,
        |if(rng_end = '9999-12-31','2020-03-14',rng_end) as rng_end
        |from
        |usertable -- 测试省略分区
        |where datediff('2020-03-14',rng_end) <= 30
        |)
        |
        |-- 计算间隔为0的天数
        |select
        |guid,
        |0 as interval ,
        |datediff(rng_end,rng_start) as times
        |from
        |x
        |
        |union all
        |-- 计算间隔为N的天数
        |-- 去重rn为null的值,无法计算
        |select
        |guid,
        |datediff(date_rn,rng_end) as interval,
        |1 as times
        |from
        |(
        |select
        |guid,
        |rng_end,
        |lead(rng_start,1,null) over(partition by guid order by rng_end) as date_rn
        |from
        |x
        |) o
        |where date_rn is not null
        |""".stripMargin).show(100,false)
  }
}
