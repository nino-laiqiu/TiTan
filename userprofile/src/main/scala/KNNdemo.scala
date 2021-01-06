import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types.{DataTypes, StructType}

import scala.collection.mutable

//label,f1,f2,f3,f4,f5 simple
//id,f1,f2,f3,f4,f5 test
object KNNdemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName(this.getClass.getSimpleName).getOrCreate()
    val schema = new StructType()
      .add("label", DataTypes.DoubleType)
      .add("f1", DataTypes.DoubleType)
      .add("f2", DataTypes.DoubleType)
      .add("f3", DataTypes.DoubleType)
      .add("f4", DataTypes.DoubleType)
      .add("f5", DataTypes.DoubleType)
    //测试数据
    val simpleframe = spark.read.schema(schema).option("header", true.toString).csv("userprofile/src/main/resources/KNN/simple.csv")
    simpleframe.createTempView("simple")
    val schema1 = new StructType()
      .add("id", DataTypes.DoubleType)
      .add("f1", DataTypes.DoubleType)
      .add("f2", DataTypes.DoubleType)
      .add("f3", DataTypes.DoubleType)
      .add("f4", DataTypes.DoubleType)
      .add("f5", DataTypes.DoubleType)

    val testframe = spark.read.schema(schema1).option("header", true.toString).csv("userprofile/src/main/resources/KNN/test.csv")
    testframe.createTempView("test")


    import org.apache.spark.sql.functions._
    //计算欧氏距离的函数
    val eudi: UserDefinedFunction = udf(
      (arr1: mutable.WrappedArray[Double], arr2: mutable.WrappedArray[Double]) => {
        val v1 = Vectors.dense(arr1.toArray)
        val v2 = Vectors.dense(arr2.toArray)
        Vectors.sqdist(v1, v2)
      }
    )
    //计算欧氏距离的函数
    val eudi1 = (arr1: mutable.WrappedArray[Double], arr2: mutable.WrappedArray[Double]) => {
      val data: Double = arr1.zip(arr2).map(it => Math.pow((it._2 - it._1), 2)).sum
      1 / (Math.pow(data, 0.5) + 1)
    }
    spark.udf.register("eudi1", eudi1)
    spark.udf.register("eudi", eudi)
    //笛卡尔积
    spark.sql(
      """
        |select
        |a.id,
        |b.label,
        |eudi(array(a.f1,a.f2,a.f3,a.f4,a.f5),array(b.f1,b.f2,b.f3,b.f4,b.f5)) as dist
        |from
        |test  a cross join simple b
        |""".stripMargin).createTempView("tmp")

    //|id |label|dist                |
    //+---+-----+--------------------+
    //|1.0|0.0  |0.17253779651421453 |
    //|2.0|0.0  |0.11696132920126338 |
    //|3.0|0.0  |0.0389439561817535  |
    //|4.0|0.0  |0.03583298491583323 |
    //|5.0|0.0  |0.03583298491583323 |
    //|1.0|0.0  |0.18660549686337075 |
    //|2.0|0.0  |0.11189119247086728 |
    spark.sql(
      """
        |select
        |id,label
        |from
        |(
        |select
        |id,label,
        |row_number() over(partition by id order by dist desc  ) as rn
        |from
        |tmp
        |) o
        |where rn = 1
        |""".stripMargin).show(100, false)

    //|id |label|
    //+---+-----+
    //|1.0|1.0  |
    //|4.0|0.0  |
    //|3.0|0.0  |
    //|2.0|1.0  |
    //|5.0|0.0  |
  }
}
