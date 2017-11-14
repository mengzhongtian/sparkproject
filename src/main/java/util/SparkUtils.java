package util;

import conf.ConfigurationManager;
import constant.Constants;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
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
}
