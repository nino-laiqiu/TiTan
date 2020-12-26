package dws

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
// 高阶聚合表
object DimensionCombination {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().config(new SparkConf().setMaster("local[*]").setAppName(this.getClass.getSimpleName)).enableHiveSupport().getOrCreate()
    spark.sql(
      s"""
        |insert into  table dw.traffic_overview_cube partition (dy='${args(0)}')
        |
        |select
        |  province,
        |  city,
        |  district,
        |  device_type,
        |  release_channel,
        |  app_version,
        |  os_name,
        |  hour_segment,
        |  count(1) as session_cnt,  --会话总数
        |  sum(pv_cnt) as pv_cnt,    --  访问页面总数
        |  sum(end_time - start_time) as time_long,
        |  count(distinct guid) as  uv_cnt  -- 用户总数
        |from
        |dw.traffic_aggr_session
        |where dy='${args(0)}'
        |group by
        |  province,
        |  city,
        |  district,
        |  device_type,
        |  release_channel,
        |  app_version,
        |  os_name,
        |  hour_segment
        |  grouping sets (
        |    (),(province),
        |    (province, city),
        |    (province, city, district),
        |    (device_type),
        |    (release_channel),
        |    (device_type, app_version),
        |    (hour_segment),
        |    (os_name, app_version)
        |  );
        |""".stripMargin)
  }
}
