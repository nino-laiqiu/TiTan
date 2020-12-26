package pojo

case class AppLogBean(
                       var account: String,
                       var appId: String,
                       var appVersion: String,
                       var carrier: String,
                       var deviceId: String,
                       var deviceType: String,
                       var eventId: String,
                       var ip: String,
                       var latitude: Double,
                       var longitude: Double,
                       var netType: String,
                       var osName: String,
                       var osVersion: String,
                       var properties: Map[String, String],
                       var releaseChannel: String,
                       var resolution: String,
                       var sessionId: String,
                       var timeStamp: Long,
                       var newsession: String = "",
                       var province: String = "",
                       var city: String = "",
                       var district: String = "",
                       // 新老用户标识为0 1
                       var newuser: Int = 0,
                       // 与评分表比较,规则:
                       // 如果今天的数据有账号,则就是账号,如果昨天的账号不为空,则就是昨天的账号,其他情况为今天的设备号
                       var guid: String = ""
                     ) {

}
