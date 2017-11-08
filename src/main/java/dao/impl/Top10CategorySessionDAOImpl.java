package dao.impl;

import dao.Top10CategorySessionDAO;
import domain.Top10CategorySession;
import jdbc.JDBCHelper;

public class Top10CategorySessionDAOImpl implements Top10CategorySessionDAO {
    public void insert(Top10CategorySession top10CategorySession) {
        String sql = "insert into top10_category_session values(?,?,?,?)";

        Object[] params = new Object[]{top10CategorySession.getTaskid(),
                top10CategorySession.getCategoryid(),
                top10CategorySession.getSessionid(),
                top10CategorySession.getClickcount()};

        JDBCHelper jdbcHelper = JDBCHelper.getInstance();
        jdbcHelper.executeUpdate(sql, params);

    }
}

