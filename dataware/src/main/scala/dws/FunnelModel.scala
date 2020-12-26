package dws

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

// 漏斗分析主题
object FunnelModel {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().config(new SparkConf().setMaster("local[*]").setAppName(this.getClass.getSimpleName)).enableHiveSupport().getOrCreate()
       spark.sql(
         """
           |select
           |  '浏览分享添加' as funnel_name,
           |  guid,
           |  comp_step
           |from
           |  (
           |    select
           |      guid as guid,
           |      -- sort_array()返回的是一个数组  regexp_extract() 只能对字符串操作
           |      case when regexp_extract(
           |        concat_ws(
           |          ',',
           |          sort_array(
           |            collect_list(
           |              concat_ws('_', cast(`timestamp` as string), eventId)
           |            )
           |          )
           |        ),
           |        '.*?(pageView).*?(share).*?(addCart).*?',
           |        3
           |      ) = 'addCart' then 3 when regexp_extract(
           |        concat_ws(
           |          ',',
           |          sort_array(
           |            collect_list(
           |              concat_ws('_', cast(`timestamp` as string), eventId)
           |            )
           |          )
           |        ),
           |        '.*?(pageView).*?(share).*?',
           |        2
           |      ) = 'share' then 2 when regexp_extract(
           |        concat_ws(
           |          ',',
           |          sort_array(
           |            collect_list(
           |              concat_ws('_', cast(`timestamp` as string), eventId)
           |            )
           |          )
           |        ),
           |        '.*?(pageView).*?',
           |        1
           |      ) = 'pageView' then 1 else 0 end as comp_step
           |    from
           |      dw.enent_app_detail
           |    where
           |      dy = '2020-12-12'
           |      and -- 漏斗步骤(
           |        (
           |          eventId = 'pageView'
           |          and properties ['pageId'] = '877'
           |        )
           |        or (
           |          eventId = 'share'
           |          and properties ['pageId'] = '791'
           |        )
           |        or (
           |          eventId = 'addCart'
           |          and properties ['pageId'] = '72'
           |           )
           |         group by guid
           |     )o
           |""".stripMargin).show(100,false)
  }
}

