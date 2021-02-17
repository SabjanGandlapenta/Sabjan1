import org.apache.spark.{SparkContext,SparkConf}
import org.apache.spark.sql.catalyst.expressions.CreateStruct.registryEntry.swap


object HelloClass {
  def main(args: Array[String]): Unit = {
     val conf = new SparkConf().setMaster("local[*]").setAppName("FirstDemo")
    val sc = new SparkContext(conf)
    sc.setLogLevel("Error")
    var rdd = sc.parallelize(Array(1,2,3,4,5))
    println(rdd.reduce(_+_))


    var pair =(1,2)
    println("before swap : "+ pair)

    val swappair = pair.swap
    println("after swap : "+ swappair)
  }
}
