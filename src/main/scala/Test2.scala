
import org.apache.spark.sql.types.{IntegerType, StructField, StructType,StringType}
import org.apache.spark.sql.{Row, SparkSession}

object Test2 {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local[*]").appName("df creat approach2").getOrCreate()
     sparkSession.sparkContext.setLogLevel("ERROR")
    val user_list1 = Seq(Row(1,"janu","delhi"),Row(2,"shayan","delhi"),Row(3,"ammu","delhi3"),Row(4,"shayanu","delhi3"))
    val user_schema1 = StructType(Array(StructField("user_id",IntegerType,true),StructField("user_name",StringType,true),StructField("user_city",StringType,true)))
    val rdd1 = sparkSession.sparkContext.parallelize(user_list1)

    val df1 = sparkSession.createDataFrame(rdd1,user_schema1)
    df1.show(1)
    val num= rdd1.getNumPartitions
      println(num)
    sparkSession.stop()
  }
}
