import java.util

import com.hankcs.hanlp.HanLP
import com.hankcs.hanlp.seg.common.Term
import org.apache.spark.ml.classification.NaiveBayes
import org.apache.spark.ml.feature.{HashingTF, IDF}
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.collection.mutable

//TF 词频
//IDF 逆文档频率 lg(文档总数/(1+出现这个词的文档数))
object TFIDFdemo {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local[*]").appName(this.getClass.getSimpleName).getOrCreate()
    import spark.implicits._
    import org.apache.spark.sql.functions._
    //分别读取三类评价数据
    //添加标签
    val frame0 = spark.read.textFile("userprofile/src/main/resources/TFIDF/good.txt").selectExpr("value as cmt", "cast(0.0 as double) as label")
    val frame1 = spark.read.textFile("userprofile/src/main/resources/TFIDF/general.txt").selectExpr("value as cmt", "cast(1.0 as double) as label")
    val frame2 = spark.read.textFile("userprofile/src/main/resources/TFIDF/poor.txt").selectExpr("value as cmt", "cast(2.0 as double) as label")
    //读取停止数据
    val framestop: Array[String] = spark.read.textFile("userprofile/src/main/resources/TFIDF/poor.txt").collect()
    //广播停止数据
    val broadcastvalue = spark.sparkContext.broadcast(framestop)
    val frame3 = frame0.union(frame1).union(frame2)
    //持久化
    frame3.cache()
    val hanrdd: Dataset[(mutable.Seq[String], Double)] = frame3.map(row => {
      val str = row.getAs[String]("cmt")
      val label = row.getAs[Double]("label")
      (str, label)
      //一个分区的数据
    }).mapPartitions(data => {
      val value = broadcastvalue.value
      //一个分区的一行数据
      data.map(txt => {
        import scala.collection.JavaConversions._
        val words: mutable.Seq[String] = HanLP.segment(txt._1).map(_.word).filter(st => (!value.contains(st)) && st.length >= 2)
        (words, txt._2)
      })
    })
    val dataFrame = hanrdd.toDF("words", "label")
    //hash映射
    val tf = new HashingTF()
      .setInputCol("words")
      .setNumFeatures(100000)
      //输出字段
      .setOutputCol("tf_vec")
    val dataFrame1 = tf.transform(dataFrame)


    // 用idf算法，将上面tf特征向量集合变成 TF-IDF特征值向量集合
    val idf = new IDF()
      .setInputCol("tf_vec")
      .setOutputCol("tf_idf_vec")
    //fit得到一个模型
    val iDFModel = idf.fit(dataFrame1)
    val tfidfVecs = iDFModel.transform(dataFrame1)
   tfidfVecs.show(100,false)
    //将总的数据分区测试集合样本集
    val array = tfidfVecs.randomSplit(Array(0.8, 0.2))
    val train = array(0)
    val test = array(1)
    //|[好评]            |0.0  |(100000,[10695],[1.0])                    |(100000,[10695],[1.791759469228055])                                                  |
    //|[不错, ....]      |0.0  |(100000,[42521,70545],[1.0,1.0])          |(100000,[42521,70545],[2.1972245773362196,2.1972245773362196])                        |
    //|[红包]            |0.0  |(100000,[10970],[1.0])                    |(100000,[10970],[2.1972245773362196])

    //训练
    // 训练朴素贝叶斯模型
    val bayes = new NaiveBayes()
      .setLabelCol("label")
      .setFeaturesCol("tf_idf_vec")
      .setSmoothing(1.0)
      .setModelType("multinomial")
    //模型
    val model = bayes.fit(train)
    //测试模型的效果
    val frame = model.transform(test)
  }
}
