package dao.impl;

import dao.PageConvertRateDao;
import domain.PageConvertRate;
import jdbc.JDBCHelper;

public class PageConvertRateDaoImpl implements PageConvertRateDao {
    @Override
    public void insert(PageConvertRate pageConvertRate) {
        JDBCHelper instance = JDBCHelper.getInstance();
        String sql = "insert into page_convert_rate values (?,?)";
        Object[] param = new Object[]{pageConvertRate.getTaskid(), pageConvertRate.getConvertRate()};
        instance.executeUpdate(sql, param);
    }
}
