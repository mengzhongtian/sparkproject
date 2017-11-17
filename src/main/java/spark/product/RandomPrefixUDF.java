package spark.product;

import org.apache.spark.sql.api.java.UDF2;

import java.util.Random;

public class RandomPrefixUDF implements UDF2<String,Integer,String>{
    @Override
    public String call(String s, Integer integer) throws Exception {
        Random random = new Random();
        int i = random.nextInt(integer);
        return i+"_"+s;

    }
}
