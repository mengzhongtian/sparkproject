package spark;

import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Optional;
import conf.ConfigurationManager;
import constant.Constants;
import dao.*;
import dao.Factory.DAOFactory;
import domain.*;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.storage.StorageLevel;
import org.stringtemplate.v4.ST;
import scala.Tuple2;
import scala.collection.immutable.Stream;
import test.MockData;
import util.*;

import java.util.*;

public class UserVisitSessionAnalyzeSpark {

    public static void main(String[] args) {
        args = new String[]{"1"};

        //构建上下文：
        SparkConf conf = new SparkConf()
                .setAppName(Constants.SPARK_APP_NAME_SESSION)
                .setMaster("local")
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .registerKryoClasses(new Class[]{CategorySortKey.class});

        JavaSparkContext jsc = new JavaSparkContext(conf);
        SQLContext sqlContext = getSQLContext(jsc.sc());

        //生成模拟数据
        mockData(jsc, sqlContext);

        TaskDao task = DAOFactory.getTask();
        Long taskid = ParamUtils.getTaskIdFromArgs(args);
        Task ta = task.findById(taskid);
        JSONObject jsonObject = JSONObject.parseObject(ta.getTask_param());
        JavaRDD<Row> row = getActionRDDByDataRange(sqlContext, jsonObject);
        row.persist(StorageLevel.MEMORY_ONLY());
        /**
         * 原始RDD
         */
        JavaPairRDD<String, Row> stringRowJavaPairRDD = getSessionid2ActionRDD(row);
        stringRowJavaPairRDD.cache();

        JavaPairRDD<String, String> sessionid2AggrInfoRDD = aggregateBySession(sqlContext, row);
        sessionid2AggrInfoRDD.cache();
        Accumulator<String> accumulator = jsc.accumulator("", new SessionAggrStatAccumulator());

        sessionid2AggrInfoRDD.count();

        /**
         * filter操作
         * 筛选符合条件的session数据
         */
        JavaPairRDD<String, String> fiter = filterSession(sessionid2AggrInfoRDD, jsonObject, accumulator);
        fiter.cache();

        JavaPairRDD<String, Row> sessionid2detailRDD = getSessionid2detailRDD(fiter, stringRowJavaPairRDD);
        sessionid2detailRDD.cache();

        /**
         * 随机抽取session
         */
        randomExtractSession(jsc, ta.getTask_id(), fiter, stringRowJavaPairRDD);


        /**
         * 计算各个范围session占比，并存入数据库
         */
        calculateAnadPersistAggrStat(accumulator.value(), taskid);

        /**
         * 获取热门品类TOPN
         */
        List<Tuple2<CategorySortKey, String>> top10CategoryList = getTopNCategory(taskid, sessionid2detailRDD);


        /**
         * 获取top10热门session
         */
        getTop10Session(jsc, ta.getTask_id(), top10CategoryList, sessionid2detailRDD);


        jsc.close();
    }

    private static void getTop10Session(JavaSparkContext jsc, final long task_id, List<Tuple2<CategorySortKey, String>> top10CategoryList, final JavaPairRDD<String, Row> sessionid2detailRDD) {
        /**
         * 第一步：将top10热门品类生成rdd
         */
        ArrayList<Tuple2<Long, Long>> top10CategoryIdList = new ArrayList<Tuple2<Long, Long>>();
        for (Tuple2<CategorySortKey, String> tuple : top10CategoryList) {
            Long categoryid = Long.valueOf(StringUtils.getFieldFromConcatString(tuple._2, "\\|", Constants.FIELD_CATEGORY_ID));
            top10CategoryIdList.add(new Tuple2<Long, Long>(categoryid, categoryid));
        }
        JavaPairRDD<Long, Long> top10CategoryIdRDD = jsc.parallelizePairs(top10CategoryIdList);

        /**
         * 第二步：计算top10品类被各session点击的次数
         */
        JavaPairRDD<String, Iterable<Row>> sessionid2detailsRDD = sessionid2detailRDD.groupByKey();
        JavaPairRDD<Long, String> categoryid2sessionCountRDD = sessionid2detailsRDD.flatMapToPair(new PairFlatMapFunction<Tuple2<String, Iterable<Row>>, Long, String>() {
            public Iterable<Tuple2<Long, String>> call(Tuple2<String, Iterable<Row>> tuple) throws Exception {
                //Tuple2<categoryid1,sessionid1=3>
                List<Tuple2<Long, String>> list = new ArrayList<Tuple2<Long, String>>();
                String sessionid = tuple._1;
                //HashMap<>(categoryid1,2)
                Map<Long, Long> map = new HashMap<Long, Long>();
                Iterator<Row> iterator = tuple._2.iterator();
                while (iterator.hasNext()) {
                    Row row = iterator.next();
                    Object o = row.get(6);
                    if (o != null) {
                        Long categoryid = Long.valueOf(String.valueOf(o));
                        Long count = map.get(categoryid);
                        if (count == null) {
                            count = 0L;
                        }
                        count++;

                        map.put(categoryid, count);

                    }

                }
                for (Map.Entry<Long, Long> entry : map.entrySet()) {
                    Long key = entry.getKey();
                    Long value = entry.getValue();
                    String s = sessionid + "," + value;
                    list.add(new Tuple2<Long, String>(key, s));
                }

                return list;
            }
        });
        //Tuple2<categoryid1,sessionid1=3>
        JavaPairRDD<Long, String> top10CategorySessionCountRDD = top10CategoryIdRDD.join(categoryid2sessionCountRDD).mapToPair(new PairFunction<Tuple2<Long, Tuple2<Long, String>>, Long, String>() {
            public Tuple2<Long, String> call(Tuple2<Long, Tuple2<Long, String>> tuple) throws Exception {
                return new Tuple2<Long, String>(tuple._1, tuple._2._2);


            }
        });

        /**
         * 分组取TopN算法实现，获取每个品类的top10活跃用户
         */

        JavaPairRDD<Long, Iterable<String>> top10CategorySessionCountsRDD = top10CategorySessionCountRDD.groupByKey();
        JavaPairRDD<String, String> top10SessionRDD = top10CategorySessionCountsRDD.flatMapToPair(new PairFlatMapFunction<Tuple2<Long, Iterable<String>>, String, String>() {
            public Iterable<Tuple2<String, String>> call(Tuple2<Long, Iterable<String>> tuple) throws Exception {
                Long categoryid = tuple._1;
                Iterator<String> iterator = tuple._2.iterator();
                String[] sessions = new String[10];
                for (int i = 0; i < 10; i++) {
                    sessions[i] = "0,0";
                }

                while (iterator.hasNext()) {
                    stringArraySort(sessions, iterator.next());
                }

                List<Tuple2<String, String>> list = new ArrayList<Tuple2<String, String>>();
                Top10CategorySessionDAO top10CategorySessionDAO = DAOFactory.getTop10CategorySessionDAO();
                for (String sessionCount : sessions) {
                    String sessionid = sessionCount.split(",")[0];
                    String count = sessionCount.split(",")[1];
                    Top10CategorySession top10CategorySession = new Top10CategorySession(task_id, categoryid, sessionid, count);
                    top10CategorySessionDAO.insert(top10CategorySession);
                    list.add(new Tuple2<String, String>(sessionid, sessionid));
                }
                return list;


            }

        });

        /**
         * 最后一步：获取top10活跃session的明细数据，并写入MySQL
         */
        JavaPairRDD<String, Tuple2<String, Row>> join = top10SessionRDD.join(sessionid2detailRDD);
        join.foreach(new VoidFunction<Tuple2<String, Tuple2<String, Row>>>() {
            public void call(Tuple2<String, Tuple2<String, Row>> tuple) throws Exception {
                Row row = tuple._2._2;
                SessionDetail sessionDetail = new SessionDetail();
                sessionDetail.setTaskid(task_id);
                sessionDetail.setUserid(row.getLong(1));
                sessionDetail.setSessionid(row.getString(2));
                sessionDetail.setPageid(row.getLong(3));
                sessionDetail.setActionTime(row.getString(4));
                sessionDetail.setSearchKeyword(row.getString(5));
                sessionDetail.setClickCategoryId(row.getLong(6));
                sessionDetail.setClickProductId(row.getLong(7));
                sessionDetail.setOrderCategoryIds(row.getString(8));
                sessionDetail.setOrderProductIds(row.getString(9));
                sessionDetail.setPayCategoryIds(row.getString(10));
                sessionDetail.setPayProductIds(row.getString(11));

                SessionDetailDao sessionDetailDAO = DAOFactory.getSessionDetailDao();
                sessionDetailDAO.insert(sessionDetail);

            }
        });


    }

    /**
     * @param "sessionid1,2","sessionid2,1" 从大到小有序的
     * @param "sessionid3,3"
     */
    private static void stringArraySort(String[] array, String s) {
        Long scount = Long.valueOf(s.split(",")[1]);
        for (int i = 0; i < array.length; i++) {
            Long count = Long.valueOf(array[i].split(",")[1]);
            if (count >= scount) {
                continue;
            } else {
                changeArray(array, s, i);
                break;
            }
        }


    }

    private static void changeArray(String[] array, String s, int i) {
        String temp;
        for (; i < array.length; i++) {
            temp = array[i];
            array[i] = s;
            s = temp;

        }


    }


    private static List<Tuple2<CategorySortKey, String>> getTopNCategory(Long taskid, JavaPairRDD<String, Row> sessionid2detailRDD) {
        System.out.println("getTopNCategory------------------------------------");


        //获取session访问过的所有品类id

        JavaPairRDD<Long, Long> flat = sessionid2detailRDD.flatMapToPair(new PairFlatMapFunction<Tuple2<String, Row>, Long, Long>() {
            public Iterable<Tuple2<Long, Long>> call(Tuple2<String, Row> tuple) throws Exception {

                List<Tuple2<Long, Long>> list = new ArrayList<Tuple2<Long, Long>>();
                Row row = tuple._2;
                Long click = row.getLong(6);
                if (click != null) {
                    list.add(new Tuple2<Long, Long>(click, click));
                }
                String order = row.getString(8);
                if (order != null) {
                    String[] split = order.split(",");
                    for (String s : split) {
                        list.add(new Tuple2<Long, Long>(Long.valueOf(s), Long.valueOf(s)));
                    }

                }
                String pay = row.getString(10);
                if (pay != null) {
                    String[] split = pay.split(",");
                    for (String s : split) {
                        list.add(new Tuple2<Long, Long>(Long.valueOf(s), Long.valueOf(s)));
                    }
                }
                return list;
            }
        });
        //去重
        flat = flat.distinct();

        /**
         * 第二步：计算各品类的点击、下单和支付的次数
         */
        //计算各品类的点击次数
        JavaPairRDD<Long, Long> clickCount = getClickCount(sessionid2detailRDD);
        //计算各个品类下单次数
        JavaPairRDD<Long, Long> orderCount = getOrderCount(sessionid2detailRDD);
        //计算各个品类支付次数
        JavaPairRDD<Long, Long> payCount = getPayCount(sessionid2detailRDD);

        /**
         * 第三步：join各品类与它的点击、下单和支付的次数
         */

        JavaPairRDD<Long, String> longStringJavaPairRDD = joinCount(flat, clickCount, orderCount, payCount);

        /**
         * 将数据映射成CategorySortKey，info的格式，然后进行二次排序。
         */
        JavaPairRDD<CategorySortKey, String> sortKey2countRDD = longStringJavaPairRDD.mapToPair(new PairFunction<Tuple2<Long, String>, CategorySortKey, String>() {
            public Tuple2<CategorySortKey, String> call(Tuple2<Long, String> tuple) throws Exception {
                String s = tuple._2;
                Long categoryid = tuple._1;
                long order = Long.valueOf(StringUtils.getFieldFromConcatString(s, "\\|", Constants.FIELD_ORDER_COUNT));
                long pay = Long.valueOf(StringUtils.getFieldFromConcatString(s, "\\|", Constants.FIELD_PAY_COUNT));
                long click = Long.valueOf(StringUtils.getFieldFromConcatString(s, "\\|", Constants.FIELD_CLICK_COUNT));
                CategorySortKey categorySortKey = new CategorySortKey(click, order, pay);
                return new Tuple2<CategorySortKey, String>(categorySortKey, s);

            }
        });
        /**
         * 排序
         */
        JavaPairRDD<CategorySortKey, String> sortedCategoryCountRDD = sortKey2countRDD.sortByKey(false);


        /**
         * take
         */
        List<Tuple2<CategorySortKey, String>> take = sortedCategoryCountRDD.take(10);
        Top10CategoryDAO top10CategoryDAO = DAOFactory.getTop10CategoryDAO();

        for (Tuple2<CategorySortKey, String> tuple : take) {
            String s = tuple._2;
            long categoryid = Long.valueOf(StringUtils.getFieldFromConcatString(
                    s, "\\|", Constants.FIELD_CATEGORY_ID));
            long click = Long.valueOf(StringUtils.getFieldFromConcatString(
                    s, "\\|", Constants.FIELD_CLICK_COUNT));
            long order = Long.valueOf(StringUtils.getFieldFromConcatString(
                    s, "\\|", Constants.FIELD_ORDER_COUNT));
            long pay = Long.valueOf(StringUtils.getFieldFromConcatString(
                    s, "\\|", Constants.FIELD_PAY_COUNT));
            Top10Category top10Category = new Top10Category();
            top10Category.setTaskid(taskid);
            top10Category.setCategoryid(categoryid);
            top10Category.setClickCount(click);
            top10Category.setOrderCount(order);
            top10Category.setPayCount(pay);

            top10CategoryDAO.inserrt(top10Category);
        }

        return take;


    }

    private static JavaPairRDD<String, Row> getSessionid2detailRDD(JavaPairRDD<String, String> fiter, JavaPairRDD<String, Row> stringRowJavaPairRDD) {
        /**
         * 第一步：获取符合条件的session访问过的所有品类
         */
        //获取符合条件的session的详细信息
        JavaPairRDD<String, Tuple2<String, Row>> join = fiter.join(stringRowJavaPairRDD);
        return join.mapToPair(new PairFunction<Tuple2<String, Tuple2<String, Row>>, String, Row>() {
            public Tuple2<String, Row> call(Tuple2<String, Tuple2<String, Row>> tuple) throws Exception {
                String sessionid = tuple._1;
                Row row = tuple._2._2;
                return new Tuple2<String, Row>(sessionid, row);


            }
        });
    }

    private static JavaPairRDD<Long, String> joinCount(JavaPairRDD<Long, Long> flat, JavaPairRDD<Long, Long> clickCount, JavaPairRDD<Long, Long> orderCount, JavaPairRDD<Long, Long> payCount) {
        JavaPairRDD<Long, Tuple2<Long, Optional<Long>>> leftClick = flat.leftOuterJoin(clickCount);
        JavaPairRDD<Long, String> map = leftClick.mapToPair(new PairFunction<Tuple2<Long, Tuple2<Long, Optional<Long>>>, Long, String>() {
            public Tuple2<Long, String> call(Tuple2<Long, Tuple2<Long, Optional<Long>>> tuple) throws Exception {
                long clickid = tuple._1;
                long clickCount = 0L;
                Optional<Long> count = tuple._2._2;
                if (count.isPresent()) {
                    clickCount = count.get();
                }
                String s = Constants.FIELD_CATEGORY_ID + "=" + clickid + "|" + Constants.FIELD_CLICK_COUNT + "=" + clickCount;
                return new Tuple2<Long, String>(clickid, s);
            }
        });

        map = map.leftOuterJoin(orderCount).mapToPair(new PairFunction<Tuple2<Long, Tuple2<String, Optional<Long>>>, Long, String>() {
            public Tuple2<Long, String> call(Tuple2<Long, Tuple2<String, Optional<Long>>> tuple) throws Exception {
                Long id = tuple._1;
                String value = tuple._2._1;
                Long count = 0L;
                Optional<Long> orderCount = tuple._2._2;
                if (orderCount.isPresent()) {
                    count = orderCount.get();
                }
                value = value + "|" + Constants.FIELD_ORDER_COUNT + "=" + count;
                return new Tuple2<Long, String>(id, value);

            }
        });

        map = map.leftOuterJoin(payCount).mapToPair(new PairFunction<Tuple2<Long, Tuple2<String, Optional<Long>>>, Long, String>() {
            public Tuple2<Long, String> call(Tuple2<Long, Tuple2<String, Optional<Long>>> tuple) throws Exception {
                Long id = tuple._1;
                String value = tuple._2._1;
                Long count = 0L;
                Optional<Long> payCount = tuple._2._2;
                if (payCount.isPresent()) {
                    count = payCount.get();
                }
                value = value + "|" + Constants.FIELD_PAY_COUNT + "=" + count;
                return new Tuple2<Long, String>(id, value);

            }
        });

        return map;


    }

    private static JavaPairRDD<Long, Long> getPayCount(JavaPairRDD<String, Row> rdd1) {

        JavaPairRDD<String, Row> filter = rdd1.filter(new Function<Tuple2<String, Row>, Boolean>() {
            public Boolean call(Tuple2<String, Row> v1) throws Exception {

                String order = v1._2.getString(10);
                return order == null ? false : true;
            }
        });
        JavaPairRDD<Long, Long> flatMap = filter.flatMapToPair(new PairFlatMapFunction<Tuple2<String, Row>, Long, Long>() {
            public Iterable<Tuple2<Long, Long>> call(Tuple2<String, Row> tuple) throws Exception {
                ArrayList<Tuple2<Long, Long>> list = new ArrayList<Tuple2<Long, Long>>();
                String pay = tuple._2.getString(10);
                String[] split = pay.split(",");
                for (String s : split) {
                    list.add(new Tuple2<Long, Long>(Long.valueOf(s), 1L));

                }
                return list;
            }
        });

        JavaPairRDD<Long, Long> resultRDD = flatMap.reduceByKey(new Function2<Long, Long, Long>() {
            public Long call(Long v1, Long v2) throws Exception {
                return v1 + v2;
            }
        });


        return resultRDD;
    }

    private static JavaPairRDD<Long, Long> getOrderCount(JavaPairRDD<String, Row> rdd1) {
        JavaPairRDD<String, Row> filter = rdd1.filter(new Function<Tuple2<String, Row>, Boolean>() {
            public Boolean call(Tuple2<String, Row> v1) throws Exception {

                String order = v1._2.getString(8);
                return order == null ? false : true;
            }
        });
        JavaPairRDD<Long, Long> flatMap = filter.flatMapToPair(new PairFlatMapFunction<Tuple2<String, Row>, Long, Long>() {
            public Iterable<Tuple2<Long, Long>> call(Tuple2<String, Row> tuple) throws Exception {
                ArrayList<Tuple2<Long, Long>> list = new ArrayList<Tuple2<Long, Long>>();
                String order = tuple._2.getString(8);
                String[] split = order.split(",");
                for (String s : split) {
                    list.add(new Tuple2<Long, Long>(Long.valueOf(s), 1L));

                }
                return list;
            }
        });

        JavaPairRDD<Long, Long> resultRDD = flatMap.reduceByKey(new Function2<Long, Long, Long>() {
            public Long call(Long v1, Long v2) throws Exception {
                return v1 + v2;
            }
        });


        return resultRDD;


    }

    /**
     * 计算各个品类点击次数
     *
     * @param rdd1
     */
    private static JavaPairRDD<Long, Long> getClickCount(JavaPairRDD<String, Row> rdd1) {

        //排除点击的品类是空的情况
        JavaPairRDD<String, Row> filter = rdd1.filter(new Function<Tuple2<String, Row>, Boolean>() {
            public Boolean call(Tuple2<String, Row> v1) throws Exception {
                Object click = v1._2.get(6);
                return click == null ? false : true;

            }
        });

        JavaPairRDD<Long, Long> map = filter.mapToPair(new PairFunction<Tuple2<String, Row>, Long, Long>() {
            public Tuple2<Long, Long> call(Tuple2<String, Row> tuple) throws Exception {
                long clickid = tuple._2.getLong(6);
                return new Tuple2<Long, Long>(clickid, 1L);
            }
        });
        JavaPairRDD<Long, Long> resultRDD = map.reduceByKey(new Function2<Long, Long, Long>() {
            public Long call(Long v1, Long v2) throws Exception {
                return v1 + v2;

            }
        });
        return resultRDD;


    }


    private static void randomExtractSession(JavaSparkContext jsc, final long task_id, JavaPairRDD<String, String> fiter, JavaPairRDD<String, Row> stringRowJavaPairRDD) {
        System.out.println("运行randomExtractSession方法");
        Random random = new Random();

        //将RDD转化为key：小时，value：拼接的str的格式
        JavaPairRDD<String, String> timeStrRDD = fiter.mapToPair(new PairFunction<Tuple2<String, String>, String, String>() {
            public Tuple2<String, String> call(Tuple2<String, String> t) throws Exception {
                String str = t._2;
                String startTime = StringUtils.getFieldFromConcatString(str, "\\|", Constants.FIELD_START_TIME);
                String dateHour = DateUtils.getDateHour(startTime);
                return new Tuple2<String, String>(dateHour, str);

            }
        });

        //把每日，每时，的seesion个数放入Map里
        Map<String, Object> countMap = timeStrRDD.countByKey();
        HashMap<String, Map<String, Long>> dateHourCountMap = new HashMap<String, Map<String, Long>>();
        for (Map.Entry<String, Object> countEntry : countMap.entrySet()) {
            String dateHour = countEntry.getKey();
            String day = dateHour.split("_")[0];
            String hour = dateHour.split("_")[1];
            Long count = Long.valueOf(String.valueOf(countEntry.getValue()));

            Map<String, Long> stringLongMap = dateHourCountMap.get(day);
            if (stringLongMap == null) {
                stringLongMap = new HashMap<String, Long>();
                dateHourCountMap.put(day, stringLongMap);
            }
            stringLongMap.put(hour, count);
        }

        /**
         * dateHourExtractMap:存储着每天，每时要提取的session的索引
         */
        int extractNumberPerDay = ConfigurationManager.getInteger(Constants.EXTRACT_SESSION_NUMBER) / dateHourCountMap.size();

        HashMap<String, Map<String, IntList>> dateHourExtractMap = new HashMap<String, Map<String, IntList>>();
        //遍历dateHourCountMap，计算每时的ssion总数
        //for循环里，每次循环表示一天的数据
        for (Map.Entry<String, Map<String, Long>> dateHourCountEntry : dateHourCountMap.entrySet()) {
            String day = dateHourCountEntry.getKey();
            Map<String, Long> hourCountMap = dateHourCountEntry.getValue();
            long session_count = 0L;
            for (long hourcount : hourCountMap.values()) {
                session_count += hourcount;

            }
            //获取改天的信息
            Map<String, IntList> hourExtractMap = dateHourExtractMap.get(day);
            //判断为空的情况
            if (hourExtractMap == null) {
                hourExtractMap = new HashMap<String, IntList>();
                //存进集合里
                dateHourExtractMap.put(day, hourExtractMap);
            }
            //不为空的情况：
            for (Map.Entry<String, Long> hourCountEntry : hourCountMap.entrySet()) {
                String hour = hourCountEntry.getKey();
                long count = hourCountEntry.getValue();
                int co = (int) count;
                int hourExtractNumber = (int) (((double) co / (double) session_count) * extractNumberPerDay);
                if (hourExtractNumber > co) {
                    hourExtractNumber = co;
                }
                IntList integers = hourExtractMap.get(hour);
                if (integers == null) {
                    integers = new IntArrayList();
                    hourExtractMap.put(hour, integers);
                }
                for (int i = 0; i < hourExtractNumber; i++) {
                    int extractIndex = random.nextInt(co);
                    while (integers.contains(extractIndex)) {
                        extractIndex = random.nextInt(co);
                    }

                    integers.add(extractIndex);
                }


            }


        }


//        final Broadcast<HashMap<String, Map<String, List<Integer>>>> broadcast = jsc.broadcast(dateHourExtractMap);
        final Broadcast<HashMap<String, Map<String, IntList>>> broadcast = jsc.broadcast(dateHourExtractMap);

        /**
         * 第三部，提取session
         */
        JavaPairRDD<String, Iterable<String>> stringIterableJavaPairRDD = timeStrRDD.groupByKey();

        JavaPairRDD<String, String> extractSessionidsRDD = stringIterableJavaPairRDD.flatMapToPair(new PairFlatMapFunction<Tuple2<String, Iterable<String>>, String, String>() {
            public Iterable<Tuple2<String, String>> call(Tuple2<String, Iterable<String>> tuple) throws Exception {
                List<Tuple2<String, String>> extractSessionids = new ArrayList<Tuple2<String, String>>();

                String dateHour = tuple._1;
                String day = dateHour.split("_")[0];
                String hour = dateHour.split("_")[1];
                Iterator<String> iterator = tuple._2.iterator();

                HashMap<String, Map<String, IntList>> value = broadcast.getValue();
                List<Integer> integers = value.get(day).get(hour);
                int index = 0;

                SessionRandomExtractDao sessionRandomExtractDao = DAOFactory.getSessionRandomExtractDao();

                //遍历
                while (iterator.hasNext()) {
                    String next = iterator.next();
                    if (integers.contains(index)) {
                        String sessionid = StringUtils.getFieldFromConcatString(next, "\\|", Constants.FIELD_SESSION_ID);
                        SessionRandomExtract sessionRandomExtract = new SessionRandomExtract();
                        sessionRandomExtract.setTask_id(task_id);
                        sessionRandomExtract.setSession_id(sessionid);
                        sessionRandomExtract.setStart_time(StringUtils.getFieldFromConcatString(next, "\\|", Constants.FIELD_START_TIME));
                        sessionRandomExtract.setSearch_keywords(StringUtils.getFieldFromConcatString(next, "\\|", Constants.FILED_SEARCH_KEYWORDS));
                        sessionRandomExtract.setClick_categoryids(StringUtils.getFieldFromConcatString(next, "\\|", Constants.FIELD_CLICK_CATEGORY_IDS));
                        sessionRandomExtractDao.insert(sessionRandomExtract);
                        extractSessionids.add(new Tuple2<String, String>(sessionid, sessionid));

                    }
                    index++;
                }
                return extractSessionids;

            }
        });
        /**
         * 第四部：抽取出来session的明细数据
         *
         */
        JavaPairRDD<String, Tuple2<String, Row>> join = extractSessionidsRDD.join(stringRowJavaPairRDD);
        join.foreach(new VoidFunction<Tuple2<String, Tuple2<String, Row>>>() {
            public void call(Tuple2<String, Tuple2<String, Row>> tuple) throws Exception {
                Row row = tuple._2._2;
                SessionDetail sessionDetail = new SessionDetail();
                sessionDetail.setTaskid(task_id);
                sessionDetail.setUserid(row.getLong(1));
                sessionDetail.setSessionid(row.getString(2));
                sessionDetail.setPageid(row.getLong(3));
                sessionDetail.setActionTime(row.getString(4));
                sessionDetail.setSearchKeyword(row.getString(5));
                sessionDetail.setClickCategoryId(row.getLong(6));
                sessionDetail.setClickProductId(row.getLong(7));
                sessionDetail.setOrderCategoryIds(row.getString(8));
                sessionDetail.setOrderProductIds(row.getString(9));
                sessionDetail.setPayCategoryIds(row.getString(10));
                sessionDetail.setPayProductIds(row.getString(11));
                SessionDetailDao sessionDetailDao = DAOFactory.getSessionDetailDao();
                sessionDetailDao.insert(sessionDetail);

            }
        });


    }

    private static void calculateAnadPersistAggrStat(String value, Long taskid) {
        System.out.println("-----------------------------------------------");
        System.out.println(value);
        long session_count = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.SESSION_COUNT));
        long visit_length_1s_3s = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_1s_3s));
        long visit_length_4s_6s = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_4s_6s));
        long visit_length_7s_9s = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_7s_9s));
        long visit_length_10s_30s = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_10s_30s));
        long visit_length_30s_60s = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_30s_60s));
        long visit_length_1m_3m = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_1m_3m));
        long visit_length_3m_10m = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_3m_10m));
        long visit_length_10m_30m = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_10m_30m));
        long visit_length_30m = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_30m));

        long step_length_1_3 = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.STEP_PERIOD_1_3));
        long step_length_4_6 = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.STEP_PERIOD_4_6));
        long step_length_7_9 = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.STEP_PERIOD_7_9));
        long step_length_10_30 = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.STEP_PERIOD_10_30));
        long step_length_30_60 = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.STEP_PERIOD_30_60));
        long step_length_60 = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.STEP_PERIOD_60));

        double visit_length_1s_3s_ratio = NumberUtils.formatDouble(
                (double) visit_length_1s_3s / (double) session_count, 2);
        double visit_length_4s_6s_ratio = NumberUtils.formatDouble(
                (double) visit_length_4s_6s / (double) session_count, 2);
        double visit_length_7s_9s_ratio = NumberUtils.formatDouble(
                (double) visit_length_7s_9s / (double) session_count, 2);
        double visit_length_10s_30s_ratio = NumberUtils.formatDouble(
                (double) visit_length_10s_30s / (double) session_count, 2);
        double visit_length_30s_60s_ratio = NumberUtils.formatDouble(
                (double) visit_length_30s_60s / (double) session_count, 2);
        double visit_length_1m_3m_ratio = NumberUtils.formatDouble(
                (double) visit_length_1m_3m / (double) session_count, 2);
        double visit_length_3m_10m_ratio = NumberUtils.formatDouble(
                (double) visit_length_3m_10m / (double) session_count, 2);
        double visit_length_10m_30m_ratio = NumberUtils.formatDouble(
                (double) visit_length_10m_30m / (double) session_count, 2);
        double visit_length_30m_ratio = NumberUtils.formatDouble(
                (double) visit_length_30m / (double) session_count, 2);

        double step_length_1_3_ratio = NumberUtils.formatDouble(
                (double) step_length_1_3 / (double) session_count, 2);
        double step_length_4_6_ratio = NumberUtils.formatDouble(
                (double) step_length_4_6 / (double) session_count, 2);
        double step_length_7_9_ratio = NumberUtils.formatDouble(
                (double) step_length_7_9 / (double) session_count, 2);
        double step_length_10_30_ratio = NumberUtils.formatDouble(
                (double) step_length_10_30 / (double) session_count, 2);
        double step_length_30_60_ratio = NumberUtils.formatDouble(
                (double) step_length_30_60 / (double) session_count, 2);
        double step_length_60_ratio = NumberUtils.formatDouble(
                (double) step_length_60 / (double) session_count, 2);
        SessionAggrStat sessionAggrStat = new SessionAggrStat();
        sessionAggrStat.setTaskid(taskid);
        sessionAggrStat.setSession_count(session_count);
        sessionAggrStat.setVisit_length_1s_3s_ratio(visit_length_1s_3s_ratio);
        sessionAggrStat.setVisit_length_4s_6s_ratio(visit_length_4s_6s_ratio);
        sessionAggrStat.setVisit_length_7s_9s_ratio(visit_length_7s_9s_ratio);
        sessionAggrStat.setVisit_length_10s_30s_ratio(visit_length_10s_30s_ratio);
        sessionAggrStat.setVisit_length_30s_60s_ratio(visit_length_30s_60s_ratio);
        sessionAggrStat.setVisit_length_1m_3m_ratio(visit_length_1m_3m_ratio);
        sessionAggrStat.setVisit_length_3m_10m_ratio(visit_length_3m_10m_ratio);
        sessionAggrStat.setVisit_length_10m_30m_ratio(visit_length_10m_30m_ratio);
        sessionAggrStat.setVisit_length_30m_ratio(visit_length_30m_ratio);
        sessionAggrStat.setStep_length_1_3_ratio(step_length_1_3_ratio);
        sessionAggrStat.setStep_length_4_6_ratio(step_length_4_6_ratio);
        sessionAggrStat.setStep_length_7_9_ratio(step_length_7_9_ratio);
        sessionAggrStat.setStep_length_10_30_ratio(step_length_10_30_ratio);
        sessionAggrStat.setStep_length_30_60_ratio(step_length_30_60_ratio);
        sessionAggrStat.setStep_length_60_ratio(step_length_60_ratio);

        SessionAggrDao sessionAggrDao = DAOFactory.getSessionAggrDao();
        sessionAggrDao.insert(sessionAggrStat);
        System.out.println(sessionAggrStat.toString());


    }

    /**
     * 获取SQLContext
     * 如果是在本地测试的话就生成SQLContext
     * 如果是在生产环境的话，就生成HiveContext
     *
     * @param sc
     * @return
     */
    private static SQLContext getSQLContext(SparkContext sc) {
        boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
        if (local) {
            return new SQLContext(sc);
        } else {
            return new HiveContext(sc);
        }

    }

    /**
     * 生成模拟数据
     */
    private static void mockData(JavaSparkContext jsc, SQLContext sqlContext) {
        boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
        if (local) {
            MockData.mock(jsc, sqlContext);
        }
    }

    /**
     * 获取指定日期范围内的用户访问行为数据
     */
    private static JavaRDD<Row> getActionRDDByDataRange(SQLContext sqlContext, JSONObject jsonObject) {
        String startDate = ParamUtils.getParam(jsonObject, Constants.PARAM_START_DATE);
        String endDate = ParamUtils.getParam(jsonObject, Constants.PARAM_END_DATE);
        String sql = "select * from user_visit_action where date>='" + startDate + "' " + "and date<= '" + endDate + "'";
        System.out.println(sql);
        DataFrame dataFrame = sqlContext.sql(sql);
        return dataFrame.javaRDD();
    }

    private static JavaPairRDD<String, Row> getSessionid2ActionRDD(JavaRDD<Row> actionRDD) {
        JavaPairRDD<String, Row> stringRowJavaPairRDD = actionRDD.mapToPair(new PairFunction<Row, String, Row>() {
            public Tuple2<String, Row> call(Row row) throws Exception {
                String sessionId = row.getString(2);
                return new Tuple2<String, Row>(sessionId, row);

            }
        });
        return stringRowJavaPairRDD;
    }

    /**
     * 聚合
     *
     * @param sqlContext
     * @param actionRDD
     * @return
     */
    private static JavaPairRDD<String, String> aggregateBySession(SQLContext sqlContext, JavaRDD<Row> actionRDD) {

        /**
         * 按照sessionId排序
         */
        JavaPairRDD<String, Row> stringRowJavaPairRDD = getSessionid2ActionRDD(actionRDD);
        final JavaPairRDD<String, Iterable<Row>> stringIterableJavaPairRDD = stringRowJavaPairRDD.groupByKey();

        JavaPairRDD<Long, String> userid2PartAggrInfoRDD = stringIterableJavaPairRDD.mapToPair(new PairFunction<Tuple2<String, Iterable<Row>>, Long, String>() {
            public Tuple2<Long, String> call(Tuple2<String, Iterable<Row>> stringIterableTuple2) throws Exception {
                String sessionId = stringIterableTuple2._1;
                Iterator<Row> iterator = stringIterableTuple2._2.iterator();
                StringBuffer search_keyword_sb = new StringBuffer("");
                StringBuffer click_category_ids_sb = new StringBuffer("");
                Long user_id = null;

                Date startTime = null;
                Date endTime = null;
                int stempLength = 0;

                while (iterator.hasNext()) {
                    Row next = iterator.next();
                    if (user_id == null) {
                        user_id = next.getLong(1);
                    }

                    String search_keyword = next.getString(5);
                    String click_category_ids = String.valueOf(next.getLong(6));
                    if (!StringUtils.isEmpty(search_keyword)) {
                        if (!search_keyword_sb.toString().contains(search_keyword)) {
                            click_category_ids_sb.append(search_keyword).append(",");
                        }
                    }
                    if (!StringUtils.isEmpty(click_category_ids)) {
                        if (!click_category_ids_sb.toString().contains(click_category_ids)) {
                            click_category_ids_sb.append(click_category_ids).append(",");
                        }
                    }
                    Date date = DateUtils.parseTime(next.getString(4));
                    if (startTime == null) {
                        startTime = date;
                    }
                    if (endTime == null) {
                        endTime = date;
                    }
                    if (date.before(startTime)) {
                        startTime = date;
                    }
                    if (date.after(endTime)) {
                        endTime = date;
                    }

                    stempLength++;


                }

                long ti = (endTime.getTime() - startTime.getTime()) / 1000;
                String search_keyword_result = StringUtils.trimComma(search_keyword_sb.toString());
                String click_category_ids_result = StringUtils.trimComma(click_category_ids_sb.toString());

                String partAggrInfo = Constants.FIELD_SESSION_ID + "=" + sessionId + "|"
                        + Constants.FILED_SEARCH_KEYWORDS + "=" + search_keyword_result + "|"
                        + Constants.FIELD_CLICK_CATEGORY_IDS + "=" + click_category_ids_result + "|"
                        + Constants.FIELD_STEP_LENGTH + "=" + stempLength + "|"
                        + Constants.FIELD_VISIT_LENGTH + "=" + ti + "|"
                        + Constants.FIELD_START_TIME + "=" + DateUtils.formatTime(startTime);
                return new Tuple2<Long, String>(user_id, partAggrInfo);


            }

        });

        /**
         * 用户数据
         */
        String sql = "select * from user_info";
        JavaRDD<Row> rowJavaRDD = sqlContext.sql(sql).javaRDD();
        JavaPairRDD<Long, Row> userid2InfoRDD = rowJavaRDD.mapToPair(new PairFunction<Row, Long, Row>() {
            public Tuple2<Long, Row> call(Row row) throws Exception {

                return new Tuple2<Long, Row>(row.getLong(0), row);
            }
        });
        /**
         * 用户数据与session数据进行join
         */

        JavaPairRDD<Long, Tuple2<String, Row>> userid2FullInfoRDD = userid2PartAggrInfoRDD.join(userid2InfoRDD);
        JavaPairRDD<String, String> stringStringJavaPairRDD = userid2FullInfoRDD.mapToPair(new PairFunction<Tuple2<Long, Tuple2<String, Row>>, String, String>() {
            public Tuple2<String, String> call(Tuple2<Long, Tuple2<String, Row>> longTuple2Tuple2) throws Exception {

                Long userid = longTuple2Tuple2._1;
                Tuple2<String, Row> stringRowTuple2 = longTuple2Tuple2._2;
                Row row = stringRowTuple2._2();
                String partAggrInfo = stringRowTuple2._1();
                String sessionid = StringUtils.getFieldFromConcatString(partAggrInfo, "\\|", Constants.FIELD_SESSION_ID);

                int age = row.getInt(3);
                String professional = row.getString(4);
                String city = row.getString(5);
                String sex = row.getString(6);
                String full = partAggrInfo + "|"
                        + Constants.FIELD_CITY + "=" + city + "|"
                        + Constants.FIELD_SEX + "=" + sex + "|"
                        + Constants.FIELD_AGE + "=" + age + "|"
                        + Constants.FIELD_PROFESSIONAL + "=" + professional;
                return new Tuple2<String, String>(sessionid, full);

            }
        });
        return stringStringJavaPairRDD;


    }


    private static JavaPairRDD<String, String> filterSession(JavaPairRDD<String, String> rdd, JSONObject param, final Accumulator<String> accumulator) {
        String startAge = ParamUtils.getParam(param, Constants.PARAM_START_AGE);
        String endAge = ParamUtils.getParam(param, Constants.PARAM_END_AGE);
        String professionals = ParamUtils.getParam(param, Constants.PARAM_PROFESSIONALS);
        String cities = ParamUtils.getParam(param, Constants.PARAM_CITIES);
        String sex = ParamUtils.getParam(param, Constants.PARAM_SEX);
        String keywords = ParamUtils.getParam(param, Constants.PARAM_KEYWORDS);
        String categoryIds = ParamUtils.getParam(param, Constants.PARAM_CATEGORY_IDS);

        String _parameter = (startAge != null ? Constants.PARAM_START_AGE + "=" + startAge + "|" : "")
                + (endAge != null ? Constants.PARAM_END_AGE + "=" + endAge + "|" : "")
                + (professionals != null ? Constants.PARAM_PROFESSIONALS + "=" + professionals + "|" : "")
                + (cities != null ? Constants.PARAM_CITIES + "=" + cities + "|" : "")
                + (sex != null ? Constants.PARAM_SEX + "=" + sex + "|" : "")
                + (keywords != null ? Constants.PARAM_KEYWORDS + keywords + "|" : "")
                + (categoryIds != null ? Constants.PARAM_CATEGORY_IDS + "=" + categoryIds + "|" : "");

        if (_parameter.endsWith("|")) {
            _parameter = _parameter.substring(0, _parameter.length() - 1);
        }
        final String parameter = _parameter;

        JavaPairRDD<String, String> filter = rdd.filter(new Function<Tuple2<String, String>, Boolean>() {
            public Boolean call(Tuple2<String, String> stringStringTuple2) throws Exception {
                String s = stringStringTuple2._2;
                //年龄
                boolean age_boo = ValidUtils.between(s, Constants.FIELD_AGE, parameter, Constants.PARAM_START_AGE, Constants.PARAM_END_AGE);
                if (!age_boo) {
                    return false;
                }
                //行业
                if (!ValidUtils.in(s, Constants.FIELD_PROFESSIONAL, parameter, Constants.PARAM_PROFESSIONALS)) {
                    return false;
                }
                //城市
                if (!ValidUtils.in(s, Constants.FIELD_CITY, parameter, Constants.PARAM_CITIES)) {
                    return false;
                }

                //性别
                if (!ValidUtils.equal(s, Constants.FIELD_SEX, parameter, Constants.PARAM_SEX)) {
                    return false;
                }

                //搜索词
                if (!ValidUtils.in(s, Constants.FILED_SEARCH_KEYWORDS, parameter, Constants.PARAM_KEYWORDS)) {
                    return false;
                }

                //品类
                if (!ValidUtils.in(s, Constants.FIELD_CLICK_CATEGORY_IDS, parameter, Constants.PARAM_CATEGORY_IDS)) {
                    return false;
                }

                accumulator.add(Constants.SESSION_COUNT);
                Long visit_length = Long.valueOf(StringUtils.getFieldFromConcatString(s, "\\|", Constants.FIELD_VISIT_LENGTH));
                Long step_length = Long.valueOf(StringUtils.getFieldFromConcatString(s, "\\|", Constants.FIELD_STEP_LENGTH));
                accumulator_visit_length(visit_length, accumulator);
                accumulator_step_length(step_length, accumulator);

                return true;


            }
        });

        return filter;
    }


    private static void accumulator_step_length(Long step_length, Accumulator<String> accumulator) {
        if (step_length >= 1 && step_length <= 3) {
            accumulator.add(Constants.STEP_PERIOD_1_3);

        } else if (step_length >= 4 && step_length <= 6) {
            accumulator.add(Constants.STEP_PERIOD_4_6);
        } else if (step_length >= 7 && step_length <= 9) {
            accumulator.add(Constants.STEP_PERIOD_7_9);
        } else if (step_length >= 10 && step_length <= 30) {
            accumulator.add(Constants.STEP_PERIOD_10_30);
        } else if (step_length > 30 && step_length <= 60) {
            accumulator.add(Constants.STEP_PERIOD_30_60);
        } else if (step_length > 60) {
            accumulator.add(Constants.STEP_PERIOD_60);
        }
    }

    private static void accumulator_visit_length(Long visit_length, Accumulator<String> accumulator) {
        if (visit_length >= 1 && visit_length <= 3) {
            accumulator.add(Constants.TIME_PERIOD_1s_3s);

        } else if (visit_length >= 4 && visit_length <= 6) {
            accumulator.add(Constants.TIME_PERIOD_4s_6s);
        } else if (visit_length >= 7 && visit_length <= 9) {
            accumulator.add(Constants.TIME_PERIOD_7s_9s);
        } else if (visit_length >= 10 && visit_length <= 30) {
            accumulator.add(Constants.TIME_PERIOD_10s_30s);
        } else if (visit_length > 30 && visit_length <= 60) {
            accumulator.add(Constants.TIME_PERIOD_30s_60s);
        } else if (visit_length > 60 && visit_length <= 180) {
            accumulator.add(Constants.TIME_PERIOD_1m_3m);
        } else if (visit_length > 180 && visit_length <= 600) {
            accumulator.add(Constants.TIME_PERIOD_3m_10m);
        } else if (visit_length > 600 && visit_length <= 1800) {
            accumulator.add(Constants.TIME_PERIOD_10m_30m);
        } else if (visit_length > 1800) {
            accumulator.add(Constants.TIME_PERIOD_30m);
        }

    }


}
