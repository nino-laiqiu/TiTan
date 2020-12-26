package dwd

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.sketch.BloomFilter
import pojo.AppLogBean

object NewAndOldUserId {
  //给T-1日的获取 添加0
  // 比较T日的数据
  def main(args: Array[String]): Unit = {

  }

  def bloomFilter(locationrdd: RDD[AppLogBean], spark: SparkSession): RDD[AppLogBean] = {
    //TODO 采用"deviceid", "account" 两个标志来过滤,是多余的,业务上关注的是账号,而不是设备,两个标志的组合是用来识别是否是同一人的
    //步骤把这俩个标志,添加到布隆过滤器中,广播出去,
    /* val set: Set[String] = spark.read.table("dw.score").where("dy='2020-12-17'").select("deviceid", "account").rdd
       .flatMap(row => {
         val deviceid = row.getAs[String]("deviceid")
         val account = row.getAs[String]("account")
         Array(deviceid, account)
       }).collect().toSet*/
    // 如果在历史id列表中包含本deviceid或本account
    /*    var flag = true
          var snew = true
        // 判断did是否存在
        if (bloomFilter.membershipTest(new Key(did.getBytes()))) {
          // 如果存在，则关闭标记，后面代码不执行，isnew=false
          flag = false
          isnew = false
        }
        // 如果did不存在，则继续判断account
        if (flag && acc != null) {
          if (bloomFilter.membershipTest(new Key(acc.getBytes()))) isnew = false
        }
        bean.isnew = if(isnew) 1 else 0
        bean
      })*/


    // TODO 采用这个广播account

    val filter: BloomFilter = spark.read.table("dw.score").where("dy='2020-12-17'").select("account").stat.bloomFilter("account", 1000000, 5)
    val bloomfliter = spark.sparkContext.broadcast(filter)

    locationrdd.mapPartitions(lter => {
      val account = bloomfliter.value
      lter.map(data => {
        //这里的数据存在没有account的日志数据,我们把这种数据视为新的用户
        if (!account.mightContain(data.account)) {
          data.newuser = 1
        }
        else {
          data.newuser = 0
        }
        data
      })
    })
  }
}
