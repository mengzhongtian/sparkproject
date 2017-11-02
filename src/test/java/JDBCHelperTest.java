import jdbc.JDBCHelper;

import java.sql.Connection;
import java.util.LinkedList;

public class JDBCHelperTest {
    public static void main(String[] args) {
        JDBCHelper instance = JDBCHelper.getInstance();

//        Connection coonection = instance.getConnection();
        LinkedList<Connection> datasource = instance.datasource;
        int size = datasource.size();
        System.out.println(size);
    }
}
