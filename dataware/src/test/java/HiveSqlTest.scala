import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

// todo 此处测试sparksql读写hive

object HiveSqlTest {
  def main(args: Array[String]): Unit = {
    //readHive
    writeHive
  }

  def readHive {
    val spark: SparkSession = SparkSession.builder().config(new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")).enableHiveSupport().getOrCreate()
    spark.sql("show databases").show()

  }

  // TODO  在Environment variables处添加变量 HADOOP_USER_NAME=root
  def writeHive {
    val spark: SparkSession = SparkSession.builder().config(new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")).enableHiveSupport().getOrCreate()

    //插入数据
    spark.sql(
      """
        |create table if not exists db_demo1.bm_1 as select deptno, name from db_demo1.bm;
        |""".stripMargin).show()

    spark.sql(
      """
        |insert into table db_demo1.bm select deptno, name from db_demo1.bm
        |""".stripMargin).show()

    //创建一张分区表
    spark.sql(
      """
        |create table if not exists  db_demo1.tb_4(
        |oid int ,
        |dt string ,
        |cost double
        |)
        |partitioned  by (dy string)
        |row format delimited fields terminated by "," ;
        |""".stripMargin).show()

    spark.sql(
      """
        |select
        |guid
        |from dw.graphData
        |group by  guid;
        |""".stripMargin).show(20)
  }
}
