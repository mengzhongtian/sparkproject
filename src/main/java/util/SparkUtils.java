package util;

import com.alibaba.fastjson.JSONObject;
import conf.ConfigurationManager;
import constant.Constants;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import sun.security.krb5.Config;
import test.MockData;

public class SparkUtils {
    /**
     * 根据当前是否本地测试的配置
     * 决定，如何设置SparkConf的master
     */
    public static void setMaster(SparkConf conf){
        boolean isLocal = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
        if (isLocal) {
            conf.setMaster("local");
        }
    }

    public static SQLContext getSQLContext(SparkContext sc) {
        boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
        if (local) {
            return new SQLContext(sc);
        } else {
            return new HiveContext(sc);
        }

    }
    public  static void mockData(JavaSparkContext jsc, SQLContext sqlContext) {
        boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
        if (local) {
            MockData.mock(jsc, sqlContext);
        }
    }


    /**
     * 获取指定日期范围内的用户访问行为数据
     */
    public static JavaRDD<Row> getActionRDDByDataRange(SQLContext sqlContext, JSONObject jsonObject) {
        String startDate = ParamUtils.getParam(jsonObject, Constants.PARAM_START_DATE);
        String endDate = ParamUtils.getParam(jsonObject, Constants.PARAM_END_DATE);
        String sql = "select * from user_visit_action where date>='" + startDate + "' " + "and date<= '" + endDate + "'";
        System.out.println(sql);
        DataFrame dataFrame = sqlContext.sql(sql);
        return dataFrame.javaRDD();
    }

}
