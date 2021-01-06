import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}


object ShopAnalysis {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName(this
      .getClass.getSimpleName).getOrCreate()

    val schema = new StructType(Array(
      StructField("shop", DataTypes.StringType),
      StructField("month", DataTypes.StringType),
      StructField("sale", DataTypes.IntegerType)
    )
    )

    val dataFrame: DataFrame = spark.read.option("header", true.toString).schema(schema).csv("userprofile/src/main/resources/shop.csv")
    dataFrame.createTempView("shoptable")

    val dataFrame1: DataFrame = spark.read.option("header", true.toString).csv("userprofile/src/main/resources/province.csv")
    dataFrame1.createTempView("provincetable")

    spark.sql(
      """
        |with y as (
        |with x as (
        |select
        |a.shop,
        |max(b.province) as province,
        |a.month,
        |sum(a.sale) as sale_cnt
        |from
        |shoptable a
        |join provincetable b on a.shop = b.shop
        |group by a.shop,a.month
        |)
        |select -- 增加字段累计金额
        |shop,province,month,sale_cnt,
        |sum(sale_cnt) over(partition by shop  order by month rows between unbounded preceding and current row)  as shop_account_cnt
        |from
        |x
        |)
        |-- 按地区和月份进行分组,求同地区同月份下所有店铺的销售金额
        |select
        |y.shop,
        |y.month,
        |y.province,
        |y.sale_cnt,
        |z.province_sale,
        |y.shop_account_cnt
        |from
        |(
        |select
        |province,
        |month,
        |sum(s.sale) as province_sale
        |from
        |shoptable s
        |join provincetable p
        |on s.shop = p.shop
        |group  by  s.month ,p.province
        |) z
        |join y on z.province = y.province and z.month = y.month
        |""".stripMargin).show(100,false)



  }
}
