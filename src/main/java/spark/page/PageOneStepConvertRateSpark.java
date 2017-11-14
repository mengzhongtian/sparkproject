package spark.page;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import util.ParamUtils;
import util.SparkUtils;

public class PageOneStepConvertRateSpark {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("PageOneStepConvertRateSpark");
        SparkUtils.setMaster(conf);
        JavaSparkContext jsc = new JavaSparkContext(conf);
        SQLContext sqlContext = SparkUtils.getSQLContext(jsc.sc());

        Long taskid = ParamUtils.getTaskIdFromArgs(args);



    }
}
