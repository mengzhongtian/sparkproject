package spark.product;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.spark.sql.api.java.UDF1;

public class RemoveRandomPrefixUDF implements UDF1<String,String> {
    @Override
    public String call(String s) throws Exception {
        String s1 = s.split("_")[1];
        return s1;

    }
}
