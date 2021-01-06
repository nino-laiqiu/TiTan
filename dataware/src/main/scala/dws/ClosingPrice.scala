package dws

import org.apache.spark.sql.SparkSession


// price_date,price
//现需通过sql获取该公司在第a天的收盘价，
// 若第a天无收盘价记录，则取该日之前，最近的一个收盘价作为第a天的收盘价
object ClosingPrice {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName(this.getClass.getSimpleName).getOrCreate()
    val frame = spark.read.option("header", true.toString).csv("file:///C:\\Users\\hp\\IdeaProjects\\TiTan\\dataware\\src\\main\\resources\\excursion.csv")
    frame.createTempView("pricetable")

    //我无语了 查找了半天我这null是字符串
    spark.sql(
      """
        |with x as (
        |select
        |collect_list(price) as arr
        |from
        |pricetable
        |where price !=  'null'
        |),
        |y as (
        |select
        |price_date,
        |price,
        |sum(label) over(order by price_date) as rn
        |from
        |(
        |select
        |price_date,
        |price,
        |if(price = 'null',0,1) as  label
        |from
        |pricetable
        |) o
        |)
        |select
        |price_date, arr[rn-1] as price
        |from
        |x,y
        |""".stripMargin)


    //方案二,采用直接where,有重复数据问题在哪:默认值的问题
    spark.sql(
      """
        |with x as (
        |select
        |price_date,price,
        |lead(price_date,1,price_date) over(order by price_date) as price_date1
        |from
        |(
        |select  -- 查找出price不为null的行
        |price_date,price
        |from
        |pricetable
        |where price != 'null'
        |) o
        |)
        |select
        |price_date,price
        |from
        |(
        |select
        |pricetable.price_date,
        |x.price
        |from
        |pricetable,x
        |where pricetable.price = 'null' and  (pricetable.price_date between x.price_date and x.price_date1)
        |
        |union all
        |
        |select
        |price_date,price
        |from
        |pricetable
        |where price != 'null'
        |) t1
        |order by  price_date
        |""".stripMargin).show(100, false)
  }
}
