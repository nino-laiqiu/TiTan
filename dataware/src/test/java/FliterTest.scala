import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


object FliterTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    val spark = SparkSession.builder().config(conf)
      .appName(this.getClass.getSimpleName)
      .enableHiveSupport()
      .master("local[*]")
      .getOrCreate()

    val frame = spark.read.table("db_demo1.app_event_log")
    // 'properties =!= null and
    //  'eventid =!= null and trim('eventid) =!= '' and
    // 'sessionid =!= null and trim('sessionid) =!= ''
    import org.apache.spark.sql.functions._
    import spark.implicits._

     frame.filter(col("sessionid").isNotNull && trim(col("sessionid")) != lit("")).show(10)
    // 表达 sessionid 不是 null 以及  trim('sessionid) 不是 ""
    //frame.filter(i=>{i.getString(0).replaceAll(" ","")!=""}).show(10)
    /*frame.filter("properties is not null" ).show(10,false)//正确
    frame.filter( s"properties is not null and ${expr("trim(sessionid) is not '' ")}  ").show(10, false)
    frame.filter('properties isNotNull ).show(10, false)
    frame.filter('properties =!= null).show(10, false) //报错
    frame.filter('properties =!= null and trim('eventid) =!= "" ).show(10,false)*/


  }

  //使用DF的filter 如 frame.filter(...) 表达 字段:sessionid 不是 null 以及  trim(sessionid) 不是 ""
}
