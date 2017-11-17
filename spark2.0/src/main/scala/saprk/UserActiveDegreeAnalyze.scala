package saprk

import org.apache.spark.sql.SparkSession

object UserActiveDegreeAnalyze {

  def main(args: Array[String]): Unit = {
    val startDate = "2017-11-16"
    val endDate = "2017-12-16"
    val spark = SparkSession.builder().master("local[2]").appName("spark2.0")
      .config("spark.sql.warehouse.dir", "C:\\mydevelop\\sparkproject")
      .getOrCreate()

    //导入saprk的隐士转换
    import spark.implicits._
    //导入spark sql的function
    import org.apache.spark.sql.functions._

    val userBaseDF = spark.read.json("user_base_info.json")

    val userActionDF = spark.read.json("user_action_log.json")

    userBaseDF.show()

    // 第一个功能：统计指定时间范围内的访问次数最多的10个用户
    // 说明：课程，所以数据不会搞的太多，但是一般来说，pm产品经理，都会抽取100个~1000个用户，供他们仔细分析


  }

}
