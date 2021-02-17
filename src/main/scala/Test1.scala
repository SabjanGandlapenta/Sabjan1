import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.CreateStruct.registryEntry.swap

object Test1{
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local[*]").appName("schema approaches").getOrCreate()
    sparkSession.sparkContext.setLogLevel("ERROR")
    val list1 = List((1,"janu","Hyd"),(2,"Ammu","Bglr"),(3,"shayan","chennai"))
    val schema_def = Seq("user_id","user_name","user_city")
    val rdd1= sparkSession.sparkContext.parallelize(list1)
    val df1= sparkSession.createDataFrame(rdd1)
    df1.show(2)
    val df2=df1.toDF(schema_def:_*)
    df2.show()


    var pair = ("Janu","Ammu")
    println("before swap : "+ pair)

    val swappair = pair.swap
    println("after swap : "+ swappair)

      sparkSession.stop()
  }
}