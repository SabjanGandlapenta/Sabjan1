import org.apache.spark.sql._

object Alljoins extends App {
  implicit val spark = SparkSession.builder().master("local[*]").getOrCreate()
  import spark.implicits._
  spark.sparkContext.setLogLevel("ERROR")

  val left = Seq((1, "A1"), (2, "A2"), (3, "A3"), (4, "A4")).toDF("id", "value")
  val right = Seq((3, "A3"), (4, "A4"), (4, "A4_1"), (5, "A5"), (6, "A6")).toDF("id", "value")

  println("LEFT")
  left.orderBy("id").show()

  println("RIGHT")
  right.orderBy("id").show()

  val joinTypes = Seq("inner", "outer", "full", "full_outer", "left", "left_outer", "right", "right_outer", "left_semi", "left_anti")

  joinTypes foreach { joinType =>
    println(s"${joinType.toUpperCase()} JOIN")
    left.join(right = right, usingColumns = Seq("id"), joinType = joinType).orderBy("id").show()
  }
}