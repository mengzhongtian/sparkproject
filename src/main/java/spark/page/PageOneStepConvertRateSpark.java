package spark.page;

import com.alibaba.fastjson.JSONObject;
import constant.Constants;
import dao.Factory.DAOFactory;
import dao.PageConvertRateDao;
import dao.TaskDao;
import domain.PageConvertRate;
import domain.Task;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import scala.Tuple2;
import util.DateUtils;
import util.NumberUtils;
import util.ParamUtils;
import util.SparkUtils;

import java.util.*;

public class PageOneStepConvertRateSpark {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("PageOneStepConvertRateSpark");
        SparkUtils.setMaster(conf);
        JavaSparkContext jsc = new JavaSparkContext(conf);
        SQLContext sqlContext = SparkUtils.getSQLContext(jsc.sc());
        SparkUtils.mockData(jsc, sqlContext);

        Long taskid = ParamUtils.getTaskIdFromArgs(args, Constants.SPARK_LOCAL_TASKID_PAGE);

        TaskDao taskDao = DAOFactory.getTaskDao();
        Task task = taskDao.findById(taskid);
        if (task == null) {
            System.out.println("can't find this task with id " + taskid);
            return;
        }

        String task_param_str = task.getTask_param();
        System.out.println("task="+task_param_str);
        JSONObject jsonObject = JSONObject.parseObject(task_param_str);
        System.out.println(jsonObject);

        JavaRDD<Row> actionRDD = SparkUtils.getActionRDDByDataRange(sqlContext, jsonObject);
        JavaPairRDD<String, Row> sessionid2actionRDD = getSessionid2actionRDD(actionRDD);
        JavaPairRDD<String, Iterable<Row>> sessionid2actionsRDD = sessionid2actionRDD.groupByKey();

        //TODO
        JavaPairRDD<String, Long> pageSplitRDD = generateAndMatchPageSplit(
                jsc, sessionid2actionsRDD, jsonObject);
        Map<String, Object> pageSplitPvMap = pageSplitRDD.countByKey();

        //计算起始页面pv
        long startPagePv = getStartPagePv(jsonObject, sessionid2actionsRDD);

        /**
         * 计算各界面的转化率
         */
        Map<String, Double> convertRateMap = computePageSplitConvertRate(
                jsonObject, pageSplitPvMap, startPagePv);


        /**
         * 持久化页面转化率：
         */
        persistConvertRate(taskid, convertRateMap);





    }

    private static void persistConvertRate(Long taskid, Map<String, Double> convertRateMap) {

        StringBuffer sb = new StringBuffer();
        for (Map.Entry<String, Double> entry : convertRateMap.entrySet()) {
            String key = entry.getKey();
            Double value = entry.getValue();
            sb.append(key).append("=").append(value).append("|");
        }
        String convertStr = sb.toString().substring(0, sb.length() - 1);
        PageConvertRateDao pageConvertRateDao = DAOFactory.getPageConvertRateDao();
        PageConvertRate pageConvertRate = new PageConvertRate();
        pageConvertRate.setTaskid(taskid);
        pageConvertRate.setConvertRate(convertStr);
        pageConvertRateDao.insert(pageConvertRate);


    }

    private static Map<String,Double> computePageSplitConvertRate(JSONObject taskParam, Map<String, Object> pageSplitPvMap, long startPagePv) {
        Map<String, Double> map = new HashMap<String, Double>();


        String[] split = ParamUtils.getParam(taskParam, Constants.PARAM_TARGET_PAGE_FLOW).split(",");

        Long front = startPagePv;
        for(int i=0;i<split.length-1;i++) {
            String s1 = split[i] + "," + split[i+1];
            long back = Long.valueOf(String.valueOf(pageSplitPvMap.get(s1)));
            double v = NumberUtils.formatDouble((double) back / (double) front, 2);
            map.put(s1, v);
            front = back;
        }
        return map;

    }

    private static long getStartPagePv(JSONObject taskParam, JavaPairRDD<String, Iterable<Row>> sessionid2actionsRDD) {
        String param = ParamUtils.getParam(taskParam, Constants.PARAM_TARGET_PAGE_FLOW);
        final String startFlow = param.split(",")[0];

        JavaRDD<Long> map = sessionid2actionsRDD.map(new Function<Tuple2<String, Iterable<Row>>, Long>() {
            @Override
            public Long call(Tuple2<String, Iterable<Row>> v1) throws Exception {
                Iterator<Row> iterator = v1._2.iterator();
                Long i = 0L;
                while (iterator.hasNext()) {
                    Row next = iterator.next();
                    long pageid = next.getLong(3);
                    if (startFlow.equals(String.valueOf(pageid))) {
                        i++;
                    }
                }
                return i;

            }
        });

        Long reduce = map.reduce(new Function2<Long, Long, Long>() {
            @Override
            public Long call(Long v1, Long v2) throws Exception {
                return v1 + v2;
            }

        });
        return reduce;


    }


    private static JavaPairRDD<String, Long> generateAndMatchPageSplit(JavaSparkContext jsc, JavaPairRDD<String, Iterable<Row>> sessionid2actionsRDD, JSONObject taskParam) {
        String param = ParamUtils.getParam(taskParam, Constants.PARAM_TARGET_PAGE_FLOW);
        final Broadcast<String> broadcast = jsc.broadcast(param);

        JavaPairRDD<String, Long> resultRDD = sessionid2actionsRDD.flatMapToPair(new PairFlatMapFunction<Tuple2<String, Iterable<Row>>, String, Long>() {
            @Override
            public Iterable<Tuple2<String, Long>> call(Tuple2<String, Iterable<Row>> tuple) throws Exception {
                List<Tuple2<String, Long>> list = new ArrayList<Tuple2<String, Long>>();
                String[] split = broadcast.getValue().split(",");

                Iterator<Row> iterator = tuple._2.iterator();
                //把row存入集合
                List<Row> rowList = new ArrayList<Row>();
                while (iterator.hasNext()) {
                    Row next = iterator.next();
                    rowList.add(next);
                }
                //从而方便进行按照点击时间的排序
                Collections.sort(rowList, new Comparator<Row>() {
                    @Override
                    public int compare(Row o1, Row o2) {
                        String s1 = o1.getString(4);
                        String s2 = o2.getString(4);
                        Date d1 = DateUtils.parseTime(s1);
                        Date d2 = DateUtils.parseTime(s2);
                        return (int) (d1.getTime() - d2.getTime());
                    }
                });

                //遍历rowList就是点击流
                String s1 = String.valueOf(rowList.get(0).getLong(3));
                for (Row row : rowList) {
                    String start = s1;
                    String end = String.valueOf(row.getLong(3));
                    String ss = start + "," + end;
                    for (int i = 0; i < split.length - 1; i++) {
                        String ss2 = split[i] + "," + split[i + 1];
                        if (ss.equals(ss2)) {
                            list.add(new Tuple2<String, Long>(ss, 1L));
                            break;
                        }
                    }


                    s1 = end;
                }
                return list;


            }
        });
        return resultRDD;


    }


    private static JavaPairRDD<String, Row> getSessionid2actionRDD(JavaRDD<Row> actionRDD) {
        return actionRDD.mapToPair(new PairFunction<Row, String, Row>() {
            @Override
            public Tuple2<String, Row> call(Row row) throws Exception {
                String string = row.getString(2);
                return new Tuple2<String, Row>(string, row);


            }
        });

    }


}
