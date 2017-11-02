package dao.impl;

import dao.SessionRandomExtractDao;
import domain.SessionRandomExtract;
import jdbc.JDBCHelper;

public class SessionRandomExtractDaoImpl implements SessionRandomExtractDao {
    public void insert(SessionRandomExtract sessionRandomExtract) {
        JDBCHelper instance = JDBCHelper.getInstance();
        String sql = "insert into session_random_extract values (?,?,?,?,?)";
        Object[] param = new Object[]{sessionRandomExtract.getTask_id(),
                sessionRandomExtract.getSession_id(), sessionRandomExtract.getStart_time(), sessionRandomExtract.getSearch_keywords()
        ,sessionRandomExtract.getClick_categoryids()};


        instance.executeUpdate(sql,param);
    }
}
