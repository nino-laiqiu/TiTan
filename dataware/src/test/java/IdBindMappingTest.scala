import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}


object IdBindMappingTest {

  /*  每日滚动更新： 设备和账号的绑定评分（表）
    设备id,登录账号,绑定得分,最近一次登录时间戳
    deviceid,account,score,ts
    这个数据的加工，应该是一个逐日滚动计算的方式

    具体计算逻辑：
        1. 加载 T-1 日的绑定评分表
        2. 计算  T日的  "设备-账号绑定评分"
        3. 综合 T-1日 和 T日的  绑定评分数据得到 T日的绑定评分数据最终结果
    T日的设备-账号绑定评分，得分规则：  每登录一次，+100分

    两日数据综合合并的逻辑
    T-1 日
    d01,a01,900
    d02,a02,800
    d01,a11,600
    d03,a03,700
    d04,a04,600

    T日
    d01,a01,200
    d03,a03,100
    d03,a13,100
    d06,a06,200
    d06,a04,100*/

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache").setLevel(Level.WARN)

    val spark = SparkSession.builder().enableHiveSupport().config(new SparkConf().setAppName(this
      .getClass.getSimpleName).setMaster("local[*]")).getOrCreate()
    import spark.implicits._
    import org.apache.spark.sql.functions._
    val schema = new StructType(Array(
      StructField("deviceid", DataTypes.StringType),
      StructField("account", DataTypes.StringType),
      StructField("timestamp", DataTypes.DoubleType),
      StructField("sessionid", DataTypes.StringType)
    ))

    val file = spark.read.format("csv").option("header", false.toString).schema(schema).load("file:///C:\\Users\\hp\\IdeaProjects\\TiTan\\dataware\\src\\main\\resources\\idbind.csv")
    val frame = file.select("deviceid", "account", "timestamp", "sessionid")
    frame.createTempView("cur_day_log")


    /*  deviceid,account,timestamp,sessionid
        d01,a01,12235,s01
        d01,a01,12266,s01
        d01,a01,12345,s02
        d01,a01,12368,s02
        d01,,12345,s03
        d01,,12376,s03
        d02,a02,12445,s04
        d02,a02,12576,s04
        d03,a03,13345,s05
        d03,a03,13376,s05
        d04,,14786,s06
        d04,,14788,s06*/

    // TODO 最好将这些标识数据写入到hive中进行查询与覆盖


    // todo 按设备和账号来分组,存在一次就+100分 ---- 今天的绑定评分
    val today_temp = spark.sql(
      """
        |select
        |deviceid,account,cast(count(distinct sessionid ) * 100 as double) as score ,max(timestamp) as  timestamp
        |from cur_day_log
        |group by deviceid,account
        |""".stripMargin)

    today_temp
      // todo  此处过滤出没有账号登录的log,为什么?
      .where("trim(account) != '' and account is not null")
      .select("deviceid", "account", "score", "timestamp")
      .createTempView("today_score")

    /* 测试 != null 和 is not null
     spark.sql(
        """
          |select
          |deviceid,account,cast(count(distinct sessionid ) * 100 as double) as score ,max(timestamp) as  timestamp
          |from cur_day_log
          |group by deviceid,account
          |""".stripMargin).where("trim(account) != '' and account != null").show()*/

    spark.sql(
      """
        |select * from  today_score
        |""".stripMargin).show()



    // TODO 昨天的绑定评分表

    val preFrame = spark.read.format("csv").option("header", true.toString).load("file:///C:\\Users\\hp\\IdeaProjects\\TiTan\\dataware\\src\\main\\resources\\idscore.csv")
    preFrame.selectExpr("deviceid", "account", "cast(timestamp as double) as timestamp ", "cast(score as double) as score")
      .createTempView("last_score")

    spark.sql(
      """
        |select * from  last_score
        |""".stripMargin).show()

    // TODO 整合昨天和今天的评分表,临时
    // 历史出现 今天出现 评分相加(取昨天的)
    // 历史出现 今天不出现 评分衰减(取昨天的)
    // 历史不出现 今天出现  取今天的

    val score_temp = spark.sql(
      """
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
        |last_score pre
        |full join
        |today_score cur
        |on pre.deviceid = cur.deviceid and  pre.account = cur.account
        |""".stripMargin)

    // 今天临时的评分表,没有新设备,已经过滤掉了
    score_temp.createTempView("score_temp")

    //  TODO 定义今天的新设备 与昨天left join

    //取出今天账号为null和空的数据
    today_temp
      // 注意这里是 or
      .where("trim(account) = '' or account is  null").createTempView("cur_may_new")


    //与昨天left join 取出 昨天设备为null的数据

    //注意字段的对齐
    //获取今天的新的设备与临时的没有新设备的进行union all
    spark.sql(
      """
        |select
        |a.deviceid,
        |a.account,
        |a.timestamp,
        |a.score
        |from
        |cur_may_new a
        |left join last_score b
        |on a.deviceid = b.deviceid
        |where b.deviceid is  null
        |
        |union all
        |select deviceid,account,timestamp,score from  score_temp
        |""".stripMargin).show()

    //发现问题:第一次的新用户的模糊性
    //存在把两个用户识别为同一个人是可能性
    // join计算量大,不断迭代复杂性,如果字段过多,SQL写法的复杂性
    // 评分衰减加权规则不合理
  }
}
