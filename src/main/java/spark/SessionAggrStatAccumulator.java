package spark;

import constant.Constants;
import org.apache.spark.AccumulatorParam;
import scala.util.parsing.combinator.testing.Str;
import util.StringUtils;

public class SessionAggrStatAccumulator implements AccumulatorParam<String> {

    public String addAccumulator(String t1, String t2) {
        return add(t1, t2);
    }

    public String addInPlace(String v1, String v2) {
        return add(v1, v2);
    }

    public String zero(String v) {
        return Constants.SESSION_COUNT + "=0|"
                + Constants.TIME_PERIOD_1s_3s + "=0|"
                + Constants.TIME_PERIOD_4s_6s + "=0|"
                + Constants.TIME_PERIOD_7s_9s + "=0|"
                + Constants.TIME_PERIOD_10s_30s + "=0|"
                + Constants.TIME_PERIOD_30s_60s + "=0|"
                + Constants.TIME_PERIOD_1m_3m + "=0|"
                + Constants.TIME_PERIOD_3m_10m + "=0|"
                + Constants.TIME_PERIOD_10m_30m + "=0|"
                + Constants.TIME_PERIOD_30m + "=0|"
                + Constants.STEP_PERIOD_1_3 + "=0|"
                + Constants.STEP_PERIOD_4_6 + "=0|"
                + Constants.STEP_PERIOD_7_9 + "=0|"
                + Constants.STEP_PERIOD_10_30 + "=0|"
                + Constants.STEP_PERIOD_30_60 + "=0|"
                + Constants.STEP_PERIOD_60 + "=0";
    }


    private String add(String v1, String v2) {
        if (StringUtils.isEmpty(v1)) {
            return v2;
        }
        String oldValue = StringUtils.getFieldFromConcatString(v1,"\\|", v2);
        if (oldValue != null) {
            String  newValue=String.valueOf(Integer.valueOf(oldValue)+1);
            v1=StringUtils.setFieldInConcatString(v1, "\\|", v2, newValue);
        }
        return v1;

    }
}
