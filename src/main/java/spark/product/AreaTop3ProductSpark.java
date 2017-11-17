package spark.product;

import com.alibaba.fastjson.JSONObject;
import conf.ConfigurationManager;
import constant.Constants;
import dao.Factory.DAOFactory;
import dao.TaskDao;
import domain.Task;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;
import util.ParamUtils;
import util.SparkUtils;

import java.util.ArrayList;
import java.util.List;

public class AreaTop3ProductSpark {
    public static void main(String[] args) {
        // 创建SparkConf
        SparkConf conf = new SparkConf()
                .setAppName("AreaTop3ProductSpark");
        SparkUtils.setMaster(conf);

        // 构建Spark上下文
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = SparkUtils.getSQLContext(sc.sc());

        // 准备模拟数据
        SparkUtils.mockData(sc, sqlContext);

        // 获取命令行传入的taskid，查询对应的任务参数
        TaskDao taskDAO = DAOFactory.getTaskDao();

        long taskid = ParamUtils.getTaskIdFromArgs(args,
                Constants.SPARK_LOCAL_TASKID_PRODUCT);
        Task task = taskDAO.findById(taskid);

        JSONObject taskParam = JSONObject.parseObject(task.getTask_param());
        String startDate = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE);
        String endDate = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE);

        // 查询用户指定日期范围内的点击行为数据（city_id，在哪个城市发生的点击行为）
        // 技术点1：Hive数据源的使用
        JavaPairRDD<Long, Row> cityid2clickActionRDD = getcityid2ClickActionRDDByDate(
                sqlContext, startDate, endDate);

        // 从MySQL中查询城市信息
        // 技术点2：异构数据源之MySQL的使用
        JavaPairRDD<Long, Row> cityid2cityInfoRDD = getcityid2CityInfoRDD(sqlContext);

        // 生成点击商品基础信息临时表
        // 技术点3：将RDD转换为DataFrame，并注册临时表
        generateTempClickProductBasicTable(sqlContext,
                cityid2clickActionRDD, cityid2cityInfoRDD);


        // 生成各区域各商品点击次数的临时表
        generateTempAreaPrdocutClickCountTable(sqlContext);
        JavaRDD<Row> areaTop3ProductRDD = getAreaTop3ProductRDD(sqlContext);
        long count = areaTop3ProductRDD.count();

        // 生成包含完整商品信息的各区域各商品点击次数的临时表
        generateTempAreaFullProductClickCountTable(sqlContext);

        sc.close();
    }



    /**
     * 生成点击商品基础信息临时表
     */

    private static void generateTempClickProductBasicTable(SQLContext sqlContext, JavaPairRDD<Long, Row> cityid2clickActionRDD, JavaPairRDD<Long, Row> cityid2cityInfoRDD) {
        JavaPairRDD<Long, Tuple2<Row, Row>> join = cityid2clickActionRDD.join(cityid2cityInfoRDD);
        JavaRDD<Row> mappedRDD = join.map(new Function<Tuple2<Long, Tuple2<Row, Row>>, Row>() {
            @Override
            public Row call(Tuple2<Long, Tuple2<Row, Row>> tuple) throws Exception {

                long cityid = tuple._1;
                Row clickAction = tuple._2._1;
                Row cityInfo = tuple._2._2;

                long productid = clickAction.getLong(1);
                String cityName = cityInfo.getString(1);
                String area = cityInfo.getString(2);

                return RowFactory.create(cityid, cityName, area, productid);
            }
        });


        //注册成一张临时表

        // 基于JavaRDD<Row>的格式，就可以将其转换为DataFrame
        List<StructField> structFields = new ArrayList<StructField>();
        structFields.add(DataTypes.createStructField("city_id", DataTypes.LongType, true));
        structFields.add(DataTypes.createStructField("city_name", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("area", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("product_id", DataTypes.LongType, true));

        StructType schema = DataTypes.createStructType(structFields);

        DataFrame df = sqlContext.createDataFrame(mappedRDD, schema);

        // 将DataFrame中的数据，注册成临时表（tmp_clk_prod_basic）
        df.registerTempTable("tmp_clk_prod_basic");


    }

    /**
     * 使用Spark SQL从MySQL中查询城市信息
     */
    private static JavaPairRDD<Long, Row> getcityid2CityInfoRDD(SQLContext sqlContext) {
        String url;
        boolean isLocal = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
        if (isLocal) {
            url = ConfigurationManager.getProperty(Constants.JDBC_URL);

        } else {
            url = ConfigurationManager.getProperty(Constants.JDBC_URL_PROD);
        }

        DataFrame cityInfoDF = sqlContext.read().format("jdbc")
                .option("url", url)
                .option("dbtable", "city_info")
                .load();

        JavaRDD<Row> cityInfoRDD = cityInfoDF.javaRDD();
        JavaPairRDD<Long, Row> cityid2cityInfoRDD = cityInfoRDD.mapToPair(new PairFunction<Row, Long, Row>() {
            @Override
            public Tuple2<Long, Row> call(Row row) throws Exception {
                long cityid = row.getLong(0);
                return new Tuple2<Long, Row>(cityid, row);
            }
        });
        return cityid2cityInfoRDD;


    }

    /**
     * 查询指定日期范围内的点击行为数据
     */
    private static JavaPairRDD<Long, Row> getcityid2ClickActionRDDByDate(SQLContext sqlContext, String startDate, String endDate) {

        // 从user_visit_action中，查询用户访问行为数据
        // 第一个限定：click_product_id，限定为不为空的访问行为，那么就代表着点击行为
        // 第二个限定：在用户指定的日期范围内的数据

        String sql =
                "SELECT "
                        + "city_id,"
                        + "click_product_id product_id "
                        + "FROM user_visit_action "
                        + "WHERE click_product_id IS NOT NULL "
                        + "AND click_product_id != 'NULL' "
                        + "AND click_product_id != 'null' "
                        + "AND action_time>='" + startDate + "' "
                        + "AND action_time<='" + endDate + "'";

        DataFrame clickActionDF = sqlContext.sql(sql);

        JavaRDD<Row> clickActionRDD = clickActionDF.javaRDD();

        JavaPairRDD<Long, Row> cityid2clickActionRDD = clickActionRDD.mapToPair(

                new PairFunction<Row, Long, Row>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Tuple2<Long, Row> call(Row row) throws Exception {
                        Long cityid = row.getLong(0);
                        return new Tuple2<Long, Row>(cityid, row);
                    }

                });

        return cityid2clickActionRDD;

    }



    private static void generateTempAreaPrdocutClickCountTable(SQLContext sqlContext) {
        String sql="SELECT "
                + "area,"
                + "product_id,"
                + "count(*) click_count, "
                + "group_concat_distinct(concat_long_string(city_id,city_name,':')) city_infos "
                + "FROM tmp_click_product_basic "
                + "GROUP BY area,product_id ";

        DataFrame dataFrame= sqlContext.sql(sql);
        dataFrame.registerTempTable("tmp_area_product_click_count");

    }

    private static JavaRDD<Row> getAreaTop3ProductRDD(SQLContext sqlContext) {

        String sql =
                "SELECT "
                        + "area,"
                        + "CASE "
                        + "WHEN area='China North' OR area='China East' THEN 'A Level' "
                        + "WHEN area='China South' OR area='China Middle' THEN 'B Level' "
                        + "WHEN area='West North' OR area='West South' THEN 'C Level' "
                        + "ELSE 'D Level' "
                        + "END area_level,"
                        + "product_id,"
                        + "click_count,"
                        + "city_infos,"
                        + "product_name,"
                        + "product_status "
                        + "FROM ("
                        + "SELECT "
                        + "area,"
                        + "product_id,"
                        + "click_count,"
                        + "city_infos,"
                        + "product_name,"
                        + "product_status,"
                        + "row_number() OVER (PARTITION BY area ORDER BY click_count DESC) rank "
                        + "FROM tmp_area_fullprod_click_count "
                        + ") t "
                        + "WHERE rank<=3";

        DataFrame df = sqlContext.sql(sql);

        return df.javaRDD();
    }

    private static void generateTempAreaFullProductClickCountTable(SQLContext sqlContext) {
        String sql = "select "
                + "tapcc.area,"
                + "tapcc.product_id,"
                + "tapcc.click_count,"
                + "tapcc.city_infos,"
                + "pi.product_name,"
                +"if(get_json_object(pi.extend_info,'product_status')='0','Self','Third Party') product_status "
                +" from tmp_area_product_click_count tapcc join product_info pi on tapcc.product_id=pi.product_id";

        DataFrame df = sqlContext.sql(sql);

        System.out.println("tmp_area_fullprod_click_count: " + df.count());

        df.registerTempTable("tmp_area_fullprod_click_count");
    }

}
