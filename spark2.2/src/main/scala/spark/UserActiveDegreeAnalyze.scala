package spark

import org.apache.spark.sql.{Dataset, Row}

object UserActiveDegreeAnalyze {

  case class UserActionLog(logId: Long, userId: Long, actionTime: String, actionType: Long, purchaseMoney: Double)

  case class UserActionLogVO(logId: Long, userId: Long, actionNum: Long)

  case class UserActionLogMoney(logId: Long, userId: Long, purchaseMoney: Double)

  def main(args: Array[String]): Unit = {
    import org.apache.spark.sql.SparkSession
    // 如果是按照课程之前的模块，或者整套交互式分析系统的架构，应该先从mysql中提取用户指定的参数（java web系统提供界面供用户选择，然后java web系统将参数写入mysql中）// 如果是按照课程之前的模块，或者整套交互式分析系统的架构，应该先从mysql中提取用户指定的参数（java web系统提供界面供用户选择，然后java web系统将参数写入mysql中）

    // 但是这里已经讲了，之前的环境已经没有了，所以本次升级从简
    // 我们就直接定义一个日期范围，来模拟获取了参数
    val startDate = "2016-09-01"
    val endDate = "2016-11-01"

    // 开始写代码
    // spark 2.0具体开发的细节和讲解，全部在从入门到精通中，这里不多说了，直接写代码
    // 要不然如果没有看过从入门到精通的话，就自己去上网查spark 2.0的入门资料

    val spark = SparkSession.builder
      .appName("UserActiveDegreeAnalyze")
      .master("local")
      //设置项目路径
      .config("spark.sql.warehouse.dir", "C:\\IdeaRepository\\sparkproject")
      .getOrCreate

    // 导入spark的隐式转换
    import spark.implicits._

    // 导入spark sql的functions
    import org.apache.spark.sql.functions._

    // 获取两份数据集
    val userBaseInfo = spark.read.json("user_base_info.json")
    val userActionLog = spark.read.json("user_action_log.json")


    /**
      * 第一个功能
      *
      * @return
      */
    def getTop10 = {
      val jonFrame = userActionLog
        .filter("actionTime >= '" + startDate + "' and actionTime <= '" + endDate + "' and actionType = 0")
        //      .join(userBaseInfo,userActionLog("userId")===userBaseInfo("userId"))

        .join(userBaseInfo, $"userId" === $"userId2")
        .groupBy(userBaseInfo("userId2"), userBaseInfo("username"))
        .agg(count(userActionLog("logId")).alias("actionCount"))
        .sort($"actionCount".desc)
        .limit(10)
      jonFrame
    }

    /**
      * 第二个功能
      */

    def getTop10Money = {
      userActionLog
        .filter("actionTime >= '" + startDate + "' and actionTime <= '" + endDate + "' and actionType = 1")
        .join(userBaseInfo, $"userId" === $"userId2")
        .groupBy(userBaseInfo("userId2"), userBaseInfo("username"))
        .agg(round(sum(userActionLog("purchaseMoney")), 2).as("moneycount"))

        .sort($"moneycount".desc)
        .limit(10)
        .show()
    }

    /**
      * 第三个功能，统计一段固定的时间：比如一个月。
      * 统计这一个月用户访问次数，减去上一个月用户访问次数，之差，最大的用户
      */

    def getTop10Max = {
      //DataFrame --> DataSet
      val userActionLogInFirstPeriod = userActionLog
        .as[UserActionLog]
        .filter("actionTime >= '2016-10-01' and actionTime <= '2016-10-31' and actionType = 0")
        .map(x => UserActionLogVO(x.logId, x.userId, 1))

      val userActionLogInSecondPeriod = userActionLog
        .as[UserActionLog]
        .filter("actionTime >= '2016-09-01' and actionTime <= '2016-09-31' and actionType = 0")
        .map(x => UserActionLogVO(x.logId, x.userId, -1))

      userActionLogInFirstPeriod.union(userActionLogInSecondPeriod)
        .groupBy($"userId")
        .agg(sum($"actionNum") as ("sumNum"))
        .sort($"sumNum".desc)
        .limit(10)
        .show()
    }

    /**
      * 第四个功能，统计最近一个周期相比上一个周期购买金额增长最多的10个用户
      */
    def getTop10MoneyIncrease = {
      val first = userActionLog.as[UserActionLog]
        .filter("actionTime >= '2016-10-01' and actionTime <= '2016-10-31' and actionType = 1")
        .map(x => UserActionLogMoney(x.logId, x.userId, x.purchaseMoney))

      //      first.show()

      val second = userActionLog
        .as[UserActionLog]
        .filter("actionTime >= '2016-09-01' and actionTime <= '2016-09-31' and actionType = 1")
        .map(x => UserActionLogMoney(x.logId, x.userId, -x.purchaseMoney))

      first
        .union(second)
        .groupBy($"userId")
        .agg(round(sum($"purchaseMoney"), 2) as ("sumMoney"))
        .sort($"sumMoney".desc)
        .limit(10)
        .show()


    }
    //    getTop10MoneyIncrease


    /**
      * 第五个功能：统计指定注册时间范围内头7天访问次数最高的10个用户
      */
    def getTop10User = {
      userActionLog
        .join(userBaseInfo, $"userId" === $"userId2")
        //        .filter("registTime >= '2016-10-01'" && "registTime <= '2016-10-31'" && "actionTime >= registTime" && "actionTime <= date_add(registTime), 7)")
        .filter($"registTime" >= "2016-10-01" && $"registTime" <= "2016-10-31" && $"actionTime" >= $"registTime" && $"actionTime" <= date_add($"registTime", 7))
        .groupBy($"userId", $"username")
        .agg(count($"logId") as ("countNum"))
        .sort($"countNum".desc)
        .limit(10)
        .show()


    }
    //    getTop10User
    /**
      * 统计指定注册时间范围内头7天购买金额最高的10个用户
      */
    def getTop10UserMoney = {
      userActionLog.join(userBaseInfo, $"userId" === $"userId2")
        .filter($"registTime" >= "2016-10-01" && $"registTime" <= "2016-10-31" && $"actionTime" >= $"registTime" && $"actionTime" <= date_add($"registTime", 7) && $"actionType" === 1)
        .groupBy($"userId", $"username")
        .agg(round(sum($"purchaseMoney"), 2) as ("sumNum"))
        .sort($"sumNum".desc)
        .limit(10)
        .show()

    }

    getTop10UserMoney

  }

}
