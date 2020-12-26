package pojo

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

object BeanUtil {
  //注意这里不能添加newsession与其他的四个属性
  def rowBean(row: Row) = {
    val account = row.getAs[String]("account")
    val appId = row.getAs[String]("appid")
    val appVersion = row.getAs[String]("appversion")
    val carrier = row.getAs[String]("carrier")
    val deviceId = row.getAs[String]("deviceid")
    val deviceType = row.getAs[String]("devicetype")
    val eventId = row.getAs[String]("eventid")
    val ip = row.getAs[String]("ip")
    val latitude = row.getAs[Double]("latitude")
    val longitude = row.getAs[Double]("longitude")
    val netType = row.getAs[String]("nettype")
    val osName = row.getAs[String]("osname")
    val osVersion = row.getAs[String]("osversion")
    val properties = row.getAs[Map[String, String]]("properties")
    val releaseChannel = row.getAs[String]("releasechannel")
    val resolution = row.getAs[String]("resolution")
    val sessionId = row.getAs[String]("sessionid")
    val timeStamp = row.getAs[Long]("timestamp")
     AppLogBean(
      account,
      appId,
      appVersion,
      carrier,
      deviceId,
      deviceType,
      eventId,
      ip,
      latitude,
      longitude,
      netType,
      osName,
      osVersion,
      properties,
      releaseChannel,
      resolution,
      sessionId,
      timeStamp,
    )
  }
}
