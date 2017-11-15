package dao.impl;

import dao.Top10CategoryDAO;
import domain.Top10Category;
import jdbc.JDBCHelper;

public class Top10CategoryDaoImpl implements Top10CategoryDAO {
    public void inserrt(Top10Category category) {

        String sql = "insert into top10_category values(?,?,?,?,?)";

        Object[] params = new Object[]{category.getTaskid(),
                category.getCategoryid(),
                category.getClickCount(),
                category.getOrderCount(),
                category.getPayCount()};

        JDBCHelper jdbcHelper = JDBCHelper.getInstance();
        jdbcHelper.executeUpdate(sql, params);

    }


}
