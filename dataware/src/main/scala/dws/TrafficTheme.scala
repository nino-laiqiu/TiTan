package dws

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

//流量概况主题
object TrafficTheme {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().config(new SparkConf().setMaster("local[*]").setAppName(this.getClass.getSimpleName)).enableHiveSupport().getOrCreate()
    spark.sql(
      s"""
        |with x as (
        |  select
        |    guid,
        |    sessionId,
        |    first_value(`timeStamp`) over (
        |      partition by guid,
        |      sessionId
        |      order by
        |        `timeStamp` rows between unbounded preceding
        |        and unbounded following
        |    ) as start_time,
        |    last_value(`timeStamp`) over (
        |      partition by guid,
        |      sessionId
        |      order by
        |        `timeStamp` rows between unbounded preceding
        |        and unbounded following
        |    ) as end_time,
        |    first_value(properties ['pageid']) over (
        |      partition by guid,
        |      sessionId
        |      order by
        |        `timeStamp` rows between unbounded preceding
        |        and unbounded following
        |    ) as in_page,
        |    last_value(properties ['pageid']) over (
        |      partition by guid,
        |      sessionId
        |      order by
        |        `timeStamp` rows between unbounded preceding
        |        and unbounded following
        |    ) as out_page,
        |    newuser,
        |    first_value(province) over (
        |      partition by guid,
        |      sessionId
        |      order by
        |        `timestamp` rows between unbounded preceding
        |        and unbounded following
        |    ) as province,
        |    first_value(city) over (
        |      partition by guid,
        |      sessionId
        |      order by
        |        `timestamp` rows between unbounded preceding
        |        and unbounded following
        |    ) as city,
        |    first_value(district) over (
        |      partition by guid,
        |      sessionId
        |      order by
        |        `timestamp` rows between unbounded preceding
        |        and unbounded following
        |    ) as district,
        |    devicetype,
        |    releasechannel,
        |    appversion,
        |    osName
        |  from
        |    dw.enent_app_detail
        |  where
        |    dy = '2020-12-11'
        |    and eventid = 'pageView'
        |)
        |insert into
        |  table dw.traffic_aggr_session partition(dy = '2020-12-11')
        |select
        |  guid,
        |  sessionId,
        |  min(start_time) as start_time,
        |  min(end_time) as end_time,
        |  min(in_page) as in_page,
        |  min(out_page) as out_page,
        |  count(1) as pv_cnt,
        |  min(newuser) as isnew,
        |  hour(
        |    from_unixtime(cast(min(start_time) / 1000 as bigint))
        |  ) as hour_segment,
        |  min(province) as province,
        |  min(city) as city,
        |  min(district) as district,
        |  min(devicetype) as device_type,
        |  min(releasechannel) as release_channel,
        |  min(appversion) as app_version,
        |  min(osname) as os_name
        |from
        |  x
        |group by
        |  guid,
        |  sessionId
        |""".stripMargin)
  }
}
