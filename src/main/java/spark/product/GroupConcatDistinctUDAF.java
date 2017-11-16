package spark.product;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import scala.collection.Seq;

import java.util.Arrays;

public class GroupConcatDistinctUDAF extends UserDefinedAggregateFunction {


    private StructType inputSchema = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("cityInfo", DataTypes.StringType, true)
    ));

    private StructType bufferSchema = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("bufferCityInfo", DataTypes.StringType, true)
    ));
    private DataType dateType = DataTypes.StringType;


    public GroupConcatDistinctUDAF() {
        super();
    }

    @Override
    //指定输入数据的字段与类型
    public StructType inputSchema() {
        return inputSchema;
    }

    @Override
    //指定缓冲数据的字段与类型
    public StructType bufferSchema() {
        return bufferSchema;
    }

    @Override
    //指定返回类型
    public DataType dataType() {
        return dateType;
    }

    @Override
    //指定是否是确定性的
    public boolean deterministic() {
        return true;
    }

    @Override
    /**
     * 初始化
     * 可以认为是，你自己在内部指定一个初始的值
     */
    public void initialize(MutableAggregationBuffer buffer) {
        buffer.update(0, "");

    }

    @Override
    /**
     * 更新
     * 可以认为是，一个一个地将组内的字段值传递进来
     * 实现拼接的逻辑
     */
    public void update(MutableAggregationBuffer buffer, Row input) {
        String s = buffer.getString(0);

        String s2 = input.getString(0);

        if (s.equals("")) {
            s += s2;
        } else {
            if (!s.contains(s2)) {
                s += "," + s2;
            }

        }
        buffer.update(0, s);


    }

    @Override
    /**
     * 合并
     * update操作，可能是针对一个分组内的部分数据，在某个节点上发生的
     * 但是可能一个分组内的数据，会分布在多个节点上处理
     * 此时就要用merge操作，将各个节点上分布式拼接好的串，合并起来
     */
    public void merge(MutableAggregationBuffer buffer1, Row buffer2) {
        String s1 = buffer1.getString(0);
        String s2 = buffer2.getString(0);
        for (String s : s2.split(",")) {
            if (!s1.contains(s)) {
                if (s1.equals("")) {
                    s1 += s;
                } else {
                    s1 += "," + s;
                }
            }

        }

        buffer1.update(0, s1);


    }

    @Override
    public Object evaluate(Row buffer) {
        return buffer.getString(0);
    }

    @Override
    public Column apply(Seq<Column> exprs) {
        return super.apply(exprs);
    }

    @Override
    public Column apply(Column... exprs) {
        return super.apply(exprs);
    }

    @Override
    public Column distinct(Seq<Column> exprs) {
        return super.distinct(exprs);
    }

    @Override
    public Column distinct(Column... exprs) {
        return super.distinct(exprs);
    }
}
