package dwd

import java.util.UUID

import ch.hsr.geohash.GeoHash
import data.GeodictUtil
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.lionsoul.ip2region.{DbConfig, DbSearcher}
import pojo.{AppLogBean, BeanUtil}

import scala.collection.mutable

// todo  ods ---> dws
object DwdEtl {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    val spark = SparkSession.builder().config(conf)
      .appName(this.getClass.getSimpleName)
      .enableHiveSupport()
      .master("local[*]")
      .getOrCreate()
    import spark.implicits._
    import org.apache.spark.sql.functions._
    //或者采用直接阅读hdfs上的数据:略
    val frame = spark.read.table("db_demo1.app_event_log")
    //account和deviceid都为null的数据
    //properties/eventid/sessionid缺少一个字段的数据
    /*frame.filter('account =!= null and 'deviceid =!= null)
      .filter(
        'properties =!= null and
          'eventid =!= null and trim('eventid) =!= '' and
          'sessionid =!= null and trim('sessionid) =!= ''
      ).show(10,false) */
    //怎么写??filter方法
    //或者采用where
    /*
       .where("nvl(account,deviceid) is not null")
       .where("(properties is not null) and  (eventid is not null and trim(eventid) != '') and (sessionid is not null and trim(sessionid) != '') ")*/

    // TODO 此处把不符合要求的数据过滤掉

    val framefilter = frame.where("nvl(account,deviceid) is not null")
      .where("(properties is not null) and  (eventid is not null and trim(eventid) != '') and (sessionid is not null and trim(sessionid) != '')")
      //在这里过滤出经纬度不符合要求的数据,防止在下面的rdd进行if的判断了
      .where('latitude > -90 and 'latitude < 90 and 'longitude > -180 and 'longitude < 180)

    val rowrdd: RDD[AppLogBean] = framefilter.rdd.map(BeanUtil.rowBean(_))
    //按照session来分组,获取每个session的时间戳来按照时间戳来排序
    val beanlistrdd: RDD[AppLogBean] = rowrdd.groupBy(_.sessionId).flatMapValues(
      appdata => {
        val beanslist: List[AppLogBean] = appdata.toList.sortBy(_.timeStamp)
        var uuid = UUID.randomUUID().toString

        // TODO SESSION分割 的切割会话,什么是一次会话,如果一次会话的时间超过了30min 就是两次会话
        //方案一
        /*   for (beanindex <- 0 until beanslist.length - 1) {
             if ((beanindex <beanslist.size -1) && (beanslist(beanindex + 1).timeStamp - beanslist(beanindex).timeStamp) < (30 * 60 * 1000)) {
               beanslist(beanindex + 1).newsession = uuid
             }
             else {
               uuid = UUID.randomUUID().toString
               beanslist(beanindex + 1).newsession = uuid
             }
           }*/
        //什么是一次会话,切分会话,切分的一次会话中时间间隔长的某段
        //方案二:
        for (beanindex <- 0 until beanslist.length - 1) {
          beanslist(beanindex).newsession = uuid
          if ((beanindex < beanslist.size - 1) && (beanslist(beanindex + 1).timeStamp - beanslist(beanindex).timeStamp) < (30 * 60 * 1000)) {
            uuid = UUID.randomUUID().toString
          }
        }
        beanslist

      }
    ).map(_._2)


    // TODO 读取本地(localhost)mysql中geo数据,并广播出去
    // TODO 如果读取mysql 注意驱动问题 改进将获取的数据上传到hive中
    val mysqlGeo: DataFrame = spark.read.format("jdbc")
      // 读取本地的mysql数据
      .option("url", "jdbc:mysql://192.168.88.3:3306/db_demo1?useUnicode=true&characterEncoding=utf-8&useSSL=false")
      .option("dbtable", "geo")
      .option("user", "root")
      .option("password", "123456")
      .load()


    val geo: Array[(String, (String, String, String))] = mysqlGeo.rdd.map(geodata => {
      val geo = geodata.getAs[String]("geo")
      val province = geodata.getAs[String]("province")
      val city = geodata.getAs[String]("city")
      val district = geodata.getAs[String]("district")
      (geo, (province, city, district))
    }
      //这里可以直接转map .collectAsMap()
    ).collect()
    var geoMap = mutable.Map[String, (String, String, String)]()
    for (elem <- geo) {
      geoMap += (elem._1 -> elem._2)
    }


    // todo 这里把 经纬度和ip地址对应格式化的数据广播出去
    val mapBroadcast = spark.sparkContext.broadcast(geoMap)

    val geoBroadcast = spark.sparkContext.broadcast(GeodictUtil.Geodict())


    //TODO 这里是对经纬度和ip地址进行数据清洗 ,查找对应的地区地址
    //赋值地址给applogbean
    val locationrdd: RDD[AppLogBean] = beanlistrdd.mapPartitions(
      beandata => {
        //ip查询
        val config = new DbConfig
        val searcher = new DbSearcher(config, geoBroadcast.value)
        beandata.map(data => {
          val tude = GeoHash.withCharacterPrecision(data.latitude, data.longitude, 6).toBase32
          //判断是否包含这个basehash
          if (mapBroadcast.value.contains(tude)) {
            val tuple: (String, String, String) = mapBroadcast.value(tude)
            data.province = tuple._1
            data.city = tuple._2
            data.district = tuple._3
            data
          }
          else {
            //  中国|0|上海|上海市|电信
            val block = searcher.memorySearch(data.ip)
            val region: String = block.getRegion
            // 对| 转义
            val strings = region.split("||\"")
            //进行判断
            if (!strings(0).equals("0") && strings.size == 5) {
              data.province = strings(2)
              data.city = strings(3)
            }
            data
          }

        })
      }
    )

    locationrdd.filter(data => data.province != null).collect().foreach(println)
    //test 测试 geo的准确性
    /* locationrdd.map(data => {
       val tude = GeoHash.withCharacterPrecision(data.latitude, data.longitude, 6).toBase32
       if (mapBroadcast.value.contains(tude)) {
         tude
       }
     }
     ).collect().foreach(println)*/

    // todo 新老用户标识
    val bloomFilter = NewAndOldUserId.bloomFilter(locationrdd, spark)

    // TODO  idmapping

    IdBindUniqueLdentification.make_guid(bloomFilter, spark)


    spark.stop()

  }
}
