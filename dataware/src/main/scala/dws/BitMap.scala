package dws

import org.apache.spark.sql.SparkSession
import sun.security.krb5.internal.PAData.SaltAndParams

//用户连续三十天--bitmap表
object BitMap {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName(this.getClass.getSimpleName).master("local[*]").enableHiveSupport().getOrCreate()
    spark.sql(
      """
        |-- 其实只要存储sum()的即可,上面用于计算
        |-- 更新今天的bitmap表,可能会有转换类型错误没有测试
        |-- 方案一:
        |with a as (
        |  select
        |    -- 会话视图层
        |    guid
        |  from
        |    dw.traffic_aggr_session
        |  where
        |    dy = '2020-12-11'
        |  group by
        |    guid
        |),
        |b as (
        |  select
        |    -- bitmap表
        |    guid,
        |    reverse(lpad(cast(bin(bitmap) as string), 31, '0')) as bitstr
        |  from
        |    dw.bitmp_30d
        |  where
        |    dt = '2020-12-17'
        |)
        |select
        |  nvl(a.guid, b.guid) as guid,
        |  conv(
        |    -- conv函数把二进制转为10进制
        |    reverse(
        |      -- 反转把最近登陆反转到结尾,离今天越近数值越小
        |      case when a.guid is not null
        |      and b.guid is not null then concat('1', substr(bitstr, 1, 30)) when a.guid is null
        |      and b.guid is not null then concat('0', substr(bitstr, 1, 30)) when b.guid is null then rpad('1', 31, '0') end
        |    ),
        |    2,
        |    10
        |  ) as bitmap
        |from
        |  a full
        |  join b on a.guid = b.guid
        |""".stripMargin).show(100,false)
  }
}
