import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.coalesce
import org.apache.spark.sql.catalyst.plans.FullOuter


object Incremental_load {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName("Incremental load").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    val list1= List((1,"abc"),(2,"cde"))
    val rdd1 = spark.sparkContext.parallelize(list1)
    val df1=spark.createDataFrame(rdd1)
    val list2= List((1,"trg"),(3,"asd"))
    val rdd2=spark.sparkContext.parallelize(list2)
    val df2 =spark.createDataFrame(list2)
    val schema_def = Seq("id","value")
    val olddf = df1.toDF(schema_def:_*)
    println("original data")
    olddf.show()
    val newdf=df2.toDF(schema_def:_*)
    newdf.show()

  println("updated data")
    val finaldf = olddf.join(newdf,Seq("id"),"full")
      .show(false)

    val innerJoinDf1 = olddf.join(newdf,Seq("id"), "inner")
     // var finaldf = olddf.alias("a").join(newdf.alias("b"),($"a.id"===$"b.id"),"Full")
    // .select(coalesce($"olddf.id",$"newdf.id").alias("id"),coalesce($"newdf.value",$"olddf.value").alias("value"))

    spark.stop()
  }
}
