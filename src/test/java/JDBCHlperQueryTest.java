import jdbc.JDBCHelper;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

public class JDBCHlperQueryTest {
    public static void main(String[] args) {
        JDBCHelper instance = JDBCHelper.getInstance();
        String sql="insert into test(name,age) values(?,?)";
        List<Object[]> param=new ArrayList<Object[]>();
        param.add(new Object[]{"mike", "27"});
        param.add(new Object[]{"mimi", "33"});
        instance.executeBatch(sql, param);


    }
}
