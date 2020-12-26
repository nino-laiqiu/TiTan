
import org.apache.hadoop.util.bloom.{BloomFilter, Key}
import org.apache.hadoop.util.hash.Hash
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.sketch
import org.sparkproject.guava.hash.Funnels

//测试布隆过滤器
object BloomFilterTest {
  def main(args: Array[String]): Unit = {
   // hadoopBloom()
   // sparkBloom()
  }

  //使用hadoop的布隆过滤器
  //todo 注意hadoop的序列化器和spark的序列化器不一致,要在config中指定序列化器

  def hadoopBloom() {
    val filter = new BloomFilter(1000000, 5, Hash.MURMUR_HASH)
    filter.add(new Key("a".getBytes()))
    filter.add(new Key("b".getBytes()))
    filter.add(new Key("c".getBytes()))
    filter.add(new Key("d".getBytes()))

    val bool = filter.membershipTest(new Key("a".getBytes))
    val bool1 = filter.membershipTest(new Key("dwd".getBytes))
    println(bool)
    println(bool1)
  }
  //使用spark的布隆过滤器
  def sparkBloom(): Unit = {
    val spark = SparkSession.builder().config(new SparkConf().setMaster("local[*]").setAppName(this.getClass.getSimpleName)).getOrCreate()
    import spark.implicits._
    val filter = spark.sparkContext.makeRDD(List("A", "B", "C", "D", "E", "F")).toDF("alphabet").stat.bloomFilter("alphabet", 100000, 0.001)
    val bool = filter.mightContain("A")
    val bool1 = filter.mightContain("R")
    println(bool)
    println(bool1)
  }
}
