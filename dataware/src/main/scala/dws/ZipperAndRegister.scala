package dws

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object ZipperAndRegister {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().config(new SparkConf().setMaster("local[*]").setAppName(this.getClass.getSimpleName)).enableHiveSupport().getOrCreate()
   spark.sql(
     """
       |select
       |  nvl(pre.first_dt, cur.cur_time) as first_dt,
       |  -- 如果之前存在那就取之前,之前不存在那就是新用户了
       |  nvl(pre.guid, cur.guid) as guid,
       |  nvl(pre.range_start, cur.cur_time) as range_start,
       |  case when pre.range_end = '9999-12-31'
       |  and cur.cur_time is null then pre.pre_time --  昨天登录,今天没有登录
       |  when pre.range_end is null then cur.cur_time -- 新用户(jion不上的)
       |  else pre.range_end -- 封闭区间保持原样
       |  end as range_end
       |from
       |  (
       |    select
       |      first_dt,
       |      guid,
       |      range_start,
       |      range_end,
       |      dy as pre_time
       |    from
       |      dw.user_act_range
       |    where
       |      dy = '2020-12-10'
       |  ) pre full
       |  join (
       |    select
       |      guid,
       |      max(dy) as cur_time
       |    from
       |      dw.traffic_aggr_session
       |    where
       |      dy = '2020-12-11'
       |    group by
       |      guid
       |  ) cur on pre.guid = cur.guid
       |union all
       |  -- 从会话层获取今日登陆的用户
       |  -- range_end封闭且今日登陆的情况
       |select
       |  first_dt as first_dt,
       |  o1.guid as guid,
       |  '2020-12-11' as range_start,
       |  '9999-12-31' as range_end
       |from
       |  (
       |    select
       |      guid,
       |      first_dt
       |    from
       |      dw.user_act_range
       |    where
       |      dy = '2020-12-10'
       |    group by
       |      guid,
       |      first_dt
       |    having
       |      max(range_end) != '9999-12-31'
       |  ) o1 -- 从会话层取出今天登陆的所有用户
       |  left semi
       |  join (
       |    select
       |      guid
       |    from
       |      dw.traffic_aggr_session
       |    where
       |      dy = '2020-12-11'
       |    group by
       |      guid
       |  ) o2 on o1.guid = o2.guid
       |""".stripMargin).show(100,false)
  }
}
