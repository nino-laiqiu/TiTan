import org.apache.spark.ml.classification.{NaiveBayes, NaiveBayesModel}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

//预测出轨率

object CheatPredict {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("出轨预测")
      .master("local[*]")
      .getOrCreate()
    //加载原始样本数据
    spark.read.option("header", "true").csv("C:\\Users\\hp\\IdeaProjects\\TiTan\\userprofile\\src\\main\\resources\\sample.csv").selectExpr("name", "job", "cast(income as double) as income", "age", "sex", "label").createTempView("simple")
    //加载测试数据
    spark.read.option("header", "true").csv("C:\\Users\\hp\\IdeaProjects\\TiTan\\userprofile\\src\\main\\resources\\test.csv").selectExpr("name", "job", "cast(income as double) as income", "age", "sex").createTempView("test")
    //原始数据的特征，加工成数值特征
   spark.sql(
      """
        |select
        |name as name ,
        |cast(
        |case job
        |     when '程序员'  then 0.0
        |     when '老师'    then 1.0
        |     when '公务员'  then 2.0
        |     end as double ) as job,
        |
        |cast(
        |case
        |  when income<10000                   then  0.0
        |  when income>=10000 and income<20000 then  1.0
        |  when income>=20000 and income<30000 then  2.0
        |  when income>=30000 and income<40000 then  3.0
        |  else 4.0
        |end as double) as income,
        |
        |cast(
        |case
        |  when  age='青年' then 0.0
        |  when  age='中年' then 1.0
        |  when  age='老年' then 2.0
        |  end as double ) as age,
        |
        |cast(if(sex='男',1,0) as double) as sex,
        |cast(if(label='出轨',0.0,1.0)  as double) as label  -- 标签
        |from
        |simple
        |""".stripMargin)
    .createTempView("simpledata")

    spark.sql(
      """
        |select
        |name as name ,
        |cast(
        |case  job
        |   when  '老师' then 0.0
        |   when '程序员' then 1.0
        |   when '公务员' then 2.0
        |   end as double
        |) as job ,
        |
        |cast(
        |case
        |  when income<10000                   then  0.0
        |  when income>=10000 and income<20000 then  1.0
        |  when income>=20000 and income<30000 then  2.0
        |  when income>=30000 and income<40000 then  3.0
        |  else 4.0
        |end as double) as income ,
        |cast(
        |case
        |  when  age='青年' then 0.0
        |  when  age='中年' then 1.0
        |  when  age='老年' then 2.0
        |  end as double ) as age,
        |
        |cast(if(sex='男',1,0) as double) as sex
        |from
        |test
        |""".stripMargin)
    .createTempView("testdata")


    // 将数值化的特征数据，向量化！（把特征转成特征向量 Vector=>DenseVector密集型向量 , SparseVector 稀疏型向量)
    val arr_Vector = (arr: mutable.WrappedArray[Double]) => {
      Vectors.dense(arr.toArray)
    }

    spark.udf.register("arr_vector", arr_Vector)

    val simple = spark.sql(
      """
        |select
        |name,
        |arr_vector(array(job,income,age,sex)) as features,
        |label
        |from
        |simpledata
        |""".stripMargin)

    val test = spark.sql(
      """
        |select
        |name,
        |arr_vector(array(job,income,age,sex)) as features
        |from
        |testdata
        |""".stripMargin)


    //算法
    // 构建对象
    val bayes = new NaiveBayes()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setSmoothing(0.01) // 拉普拉斯平滑系数
      .setPredictionCol("cheat")

    val model = bayes.fit(simple)
    //持久化训练集
    model.save("C:\\Users\\hp\\IdeaProjects\\TiTan\\userprofile\\src\\main\\resources\\mode")
    val naiveBayesModel = NaiveBayesModel.load("C:\\Users\\hp\\IdeaProjects\\TiTan\\userprofile\\src\\main\\resources\\mode")
    val frame = naiveBayesModel.transform(test)
    frame.show(10, false)
  }
}
