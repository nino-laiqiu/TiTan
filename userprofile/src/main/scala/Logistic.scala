import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.MinMaxScaler
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import scala.collection.mutable


//流失率预测
object Logistic {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    val spark = SparkSession.builder().appName(this.getClass.getSimpleName).master("local[*]").getOrCreate()
    val simple = spark.read.option("header", "true").option("inferSchema", true).csv("userprofile/src/main/resources/Logistic/ loss_predict.csv")

    //将数组转为向量
    val arr2Vec = (arr: mutable.WrappedArray[Double]) => Vectors.dense(arr.toArray)

    //label,gid,3_cs,15_cs,3_xf,15_xf,3_th,15_th,3_hp,15_hp,3_cp,15_cp,last_dl,last_xf
    import spark.implicits._
    spark.udf.register("arr2Vec", arr2Vec)
    //将样本数据向量化
    val frame = simple.selectExpr("label", "arr2Vec(array(3_cs,15_cs,3_xf,15_xf,3_th,15_th,3_hp,15_hp,3_cp,15_cp,last_dl,last_xf)) as vec")
    val minMaxScaler = new MinMaxScaler()
      .setInputCol("vec")
      .setOutputCol("features")
    //生成模型.训练样本
    val maxScalerModel = minMaxScaler.fit(frame)
    val minMaxFrame = maxScalerModel.transform(frame).drop("vec")
    minMaxFrame.show(100, false)
    //|0.0 |[0.6666666666666666,0.9354838709677419,0.3333333333333333,0.7692307692307693,0.0,0.3333333333333333,0.7142857142857142,0.7894736842105263,0.3333333333333333,0.5,0.0,0.0]                  |
    //|0.0 |[0.7777777777777777,0.967741935483871,0.4444444444444444,0.7692307692307693,0.5,0.6666666666666666,0.7857142857142857,0.8421052631578947,0.6666666666666666,0.75,0.0,0.0]                  |
    //|0.0 |[0.8888888888888888,0.9032258064516129,0.6666666666666666,0.8461538461538463,1.0,0.6666666666666666,0.8571428571428571,0.894736842105263,0.3333333333333333,0.25,0.0,0.07692307692307693]  |

    //构建逻辑回归算法
    val logisticRegression = new LogisticRegression()
      .setFeaturesCol("features")
      .setLabelCol("label")
    val  Array(train,test) = minMaxFrame.randomSplit(Array(0.8, 0.2))

    val logisticRegressionModel = logisticRegression.fit(train)
    val frame1: DataFrame = logisticRegressionModel.transform(test)
    frame1.select("label","prediction").show(100,false)
    //|label|prediction|
    //+-----+----------+
    //|0.0  |0.0       |
    //|0.0  |0.0       |
    //|1.0  |1.0       |

    //计算准确率select count(*)  from where label = prediction / select count(*)




  }
}
