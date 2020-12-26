package data

import ch.hsr.geohash.GeoHash
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

//经纬度字典表存入数据库
object Geo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName(" ")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val rdd = spark.sparkContext.textFile("file:///C:\\Users\\hp\\IdeaProjects\\TiTan\\dataware\\src\\main\\resources\\geodict.txt")
    import spark.implicits._
    val maprdd = rdd.map(data => {
      val txt = data.split(" ")
      val str = txt(4).split(":")
      //第一个是维度 第二个是精度
      val geo: String = GeoHash.withCharacterPrecision(txt(3).toDouble, txt(2).toDouble, 6).toBase32
      (geo, str(0), str(1), str(2))
    })
    val frame = maprdd.toDF("geo", "province", "city", "district")
    frame.write.format("jdbc")
      // 注意格式
      .option("url", "jdbc:mysql://linux03:3306/db_demo1?useUnicode=true&characterEncoding=utf-8&useSSL=false")
      .option("dbtable", "geo")
      .option("user", "root")
      .option("password", "123456")
      .mode("append")
      .save()
  }
}
