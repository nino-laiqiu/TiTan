package dwd

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import pojo.AppLogBean

object IdBindUniqueLdentification {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().config(new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")).enableHiveSupport().getOrCreate()
    // 发现问题:初始定义的字段过少,导致要重新写etl

    // TODO 生成一张原始的评分表
    //make_new(spark)
  }
    def make_new(spark: SparkSession): Unit ={
      spark.sql(
        """
          |insert into table dw.score partition(dy="2020-12-16")
          |select
          |deviceid,account,cast(count(distinct sessionid ) * 100 as double) as score ,max(timestamp) as  timestamp
          |from db_demo1.app_event_log
          |group by deviceid,account
          |""".stripMargin).show()
    }

    // TODO 依据昨天的评分表生成今天的guid
    def make_guid(locationrdd: RDD[AppLogBean],spark: SparkSession): Unit ={

           //读取t-1的评分表,并广播出去

           // 并且获取评分最高的一组
           spark.read.table("dw.score").where("dy='2020-12-16'").createTempView("pre_score")

           //取分数最大的 deviceid,account 组合 按照deviceid来分组,获取这个设备最大的账号
           spark.sql(
             """
               |select
               |deviceid,account
               |from
               |(
               |select
               |deviceid,account
               |row_number() over(partition by  deviceid order by score desc,timestamp desc) as rn
               |from
               |pre_score pre
               |) t1
               |where t1.rn = 1
               |
               |""".stripMargin).createTempView("idbind")
      import  spark.implicits._

      locationrdd.toDF().createTempView("regioned")


      //为今天的log添加guid 字段
     /* 计算思想：
       1. 将日志数据 left join 绑定评分表，连接条件：  deviceid
       2. 如果日志中有account，则 guid= 日志表的account
       3. 如果日志中没有account，而评分表中有account，则guid = 评分表的account
       4. 如果日志和评分表中都没有account，则 guid = 设备id*/

      // 添加到hive表中,如果之后还有其他的etl步骤,可采用广播变量来设置guid

       spark.sql(
        """
          |
          |INSERT INTO TABLE dw.log PARTITION(dy='2020-12-18')
          |
          |select
          | regioned.account          ,
          | regioned.appid            ,
          | regioned.appversion       ,
          | regioned.carrier          ,
          | regioned.deviceid         ,
          | regioned.devicetype       ,
          | regioned.eventid          ,
          | regioned.ip               ,
          | regioned.latitude         ,
          | regioned.longitude        ,
          | regioned.nettype          ,
          | regioned.osname           ,
          | regioned.osversion        ,
          | regioned.properties       ,
          | regioned.releasechannel   ,
          | regioned.resolution       ,
          | regioned.sessionid        ,
          | regioned.timestamp        ,
          | regioned.newsession       ,
          | regioned.province   	    ,
          | regioned.city             ,
          | regioned.district         ,
          | regioned.isnew            ,
          | case
          |   when regioned.account is not null then regioned.account
          |   when idbind.account is not null then idbind.account
          |   else regioned.deviceid
          | end as guid
          |
          |from regioned left join bind on idbind.deviceid=regioned.deviceid
          |
          |""".stripMargin)
    }

    // TODO 更新今天的评分表

    def today_bind(spark: SparkSession): Unit ={

      val today_temp = spark.sql(
        """
          |select
          |deviceid,account,cast(count(distinct sessionid ) * 100 as double) as score ,max(timestamp) as  timestamp
          |from cur_day_log
          |group by deviceid,account
          |""".stripMargin)

      // TODO 生成今天的评分表,过滤出账号为空的记录,假设这个设备正好是新设备,
      //  确实要过滤出来防止重复,如果不是新的设备那么之前的score表一定有记录
      today_temp
        .where("trim(account) != ''  and account is not null")
        .select("deviceid", "account", "score", "timestamp")
        .createTempView("today_score")

      //昨天的评分表
      spark.read.table("dw.score").where("dy='2020-12-16'").createTempView("pre_score")

      // TODO 取出今天的新设备,当然这样过滤不能说明这个设备号在昨天没有/有登录,定义新设备是这个设备没有账号登录过,定义这个设备为新设备.
      //   当然这个设备在昨天可能与某个账号同时出现过,所以再次与评分表join过滤
      today_temp
        // 注意这里是 or
        .where("trim(account) = '' or account is null").createTempView("cur_may_new")

       // nvl() 中必定是要么全部是昨天的要么全部是今天的
      spark.sql(
        """
          |INSERT INTO TABLE dw.score PARTITION(dy='2020-12-18')
          |
          |select
          |a.deviceid,
          |a.account,
          |a.timestamp,
          |a.score
          |from
          |cur_may_new a
          |left join pre_score b
          |on a.deviceid = b.deviceid
          |where b.deviceid is  null
          |
          |union all
          |
          |select
          |nvl(pre.deviceid,cur.deviceid) as deviceid, -- 设备
          |nvl(pre.account,cur.account) as account, -- 账号
          |nvl(cur.timestamp,pre.timestamp) as timestamp, -- 最近一次访问时间戳
          |case
          |when pre.deviceid is not null and cur.deviceid is not null then pre.score+cur.score
          |when pre.deviceid is not null and cur.deviceid is null then pre.score*0.5
          |when pre.deviceid is  null and cur.deviceid is not  null then cur.score
          |end as score
          |from
          |pre_score pre
          |full join
          |today_score cur
          |on pre.deviceid = cur.deviceid and  pre.account = cur.account
          |""".stripMargin).show()
    }
  }

