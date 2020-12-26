package dws

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import spire.std.tuples

import scala.collection.{immutable, mutable}
import scala.collection.mutable.ListBuffer

object FactorAnalysis {
  //归因分析:对用户复杂的消费行为路径的分析
  //目标事件 e6
  //待归因事件 'e1','e3','e5'
  //数据:见项目factor.csv

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName(this.getClass.getSimpleName).master("local[*]").enableHiveSupport().getOrCreate()
    spark.read.format("csv").load("file:///C:\\Users\\hp\\IdeaProjects\\TiTan\\dataware\\src\\main\\resources\\factor.csv").toDF("guid", "event", "timestamp").createTempView("factor")
    //使用SQL的方式聚合出每个用户的事件信息


    spark.sql(
      """
        |select
        |guid,
        |sort_array(collect_list(concat_ws('_',timestamp,event) )) as eventId
        |from
        |factor
        |where event in ('e1','e3','e5','e6')
        |group by guid
        |having array_contains(collect_list(event),'e6')
        |""".stripMargin).createTempView("attr_event")

    spark.udf.register("first_time", firstTime)
    spark.udf.register("first_time1", firstTime1)
    spark.udf.register("Thelastattribution", Thelastattribution)
    spark.udf.register("linearattribution", linearattribution)
    spark.udf.register("decay", decay)

    spark.sql(
      """
        |select
        |guid,
        |linearattribution(eventId,array('e1','e3','e5')) as attribute
        |from
        |attr_event
        |""".stripMargin).show(10, false)

  }

  // 其实只要返回第一个值即可,发现问题:可能会用多个"e6"
  val firstTime = (event_list: mutable.WrappedArray[String], attribute: mutable.WrappedArray[String]) => {
    //判断首次出现的事件'e1','e3','e5'
    val event = event_list.map(data => {
      val strings = data.split("_")
      (strings(0), strings(1))
    })

    val array = for (i <- event; j <- attribute if (i._2 == j)) yield (i, j)
    // 按照 value 来去重获取第一次出现的值
    implicit var sort = Ordering[(String, String)].on[(String, (String, String))](t => t._2).reverse
    var set = mutable.TreeSet[((String, String), String)]()
    for (elem <- array) {
      set += elem
    }
    set.head._2
  }

  val firstTime1 = (event_list: mutable.WrappedArray[String], attribute: mutable.WrappedArray[String]) => {
    var tuple: (List[String], List[String]) = event_list.toList.map(_.split("_")(1)).span(_ != "e6")
    var temp: (List[String], List[String]) = tuple
    var strings = ListBuffer[String]()
    while (temp._2.nonEmpty) {
      if (temp._1.nonEmpty) {
        strings += temp._1.head
      }
      temp = temp._2.tail.span(_ != "e6")
    }
    strings
  }
  // 末次触点归因
  val Thelastattribution = (event_list: mutable.WrappedArray[String], attribute: mutable.WrappedArray[String]) => {
    var tuple: (List[String], List[String]) = event_list.toList.map(_.split("_")(1)).span(_ != "e6")
    var strings = ListBuffer[String]()
    while (tuple._2.nonEmpty) {
      if (tuple._1.nonEmpty) {
        //init 方法是返回除最后一个元素之外的全部元素
        strings += tuple._1.last
      }
      tuple = tuple._2.tail.span(_ != "e6")
    }
    strings
  }

  //线性归因
  val linearattribution = (event_list: mutable.WrappedArray[String], attribute: mutable.WrappedArray[String]) => {
    //要对每段的匹配到的去重
    var tuple: (List[String], List[String]) = event_list.toList.map(_.split("_")(1)).span(_ != "e6")
    var buffer = ListBuffer[(String, Int)]()
    while (tuple._2.nonEmpty) {
      if (tuple._1.nonEmpty) {
        val value = tuple._1.toSet
        val size = value.size
        for (elem <- value) {
          buffer += ((elem, 100 / size))
        }
      }
      tuple = tuple._2.tail.span(_ != "e6")
    }
    buffer
  }
  //时间衰减归因decay
  val decay = (event_list: mutable.WrappedArray[String], attribute: mutable.WrappedArray[String]) => {
    var tuple: (List[String], List[String]) = event_list.toList.map(_.split("_")(1)).span(_ != "e6")
    var buffer = ListBuffer[(String, Double)]()
    while (tuple._2.nonEmpty) {
      if (tuple._1.nonEmpty) {
        val list = tuple._1.toSet.toList
        val seq = for (i <- list.indices) yield Math.pow(0.9, i)
        val tuples1: immutable.Seq[(String, Double)] = for (j <- list.indices) yield (list(j), seq(j) / seq.sum)
        for (elem <- tuples1) {
          buffer += elem
        }
      }
      tuple = tuple._2.tail.span(_ != "e6")
    }
    buffer
  }
  //|guid|attribute                                                                       |
  //+----+--------------------------------------------------------------------------------+
  //|g02 |[[e3, 0.5263157894736842], [e1, 0.4736842105263158], [e1, 1.0]]                 |
  //|g01 |[[e1, 0.5263157894736842], [e3, 0.4736842105263158]]                            |
  //|g03 |[[e3, 0.36900369003690037], [e1, 0.33210332103321033], [e5, 0.2988929889298893]]|

  //位置归因
  //同理只要去第一个和最后一个即可不再写了

  //末次非触点归因分析,排除最后一个即可,例如:list.init.last取出第二个元素
}
