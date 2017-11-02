package dao.impl;

import dao.SessionAggrDao;
import domain.SessionAggrStat;
import jdbc.JDBCHelper;

public class SessionAggrDaoImpl implements SessionAggrDao {
    public void insert(SessionAggrStat sessionAggrStat) {
        JDBCHelper instance = JDBCHelper.getInstance();
        String sql="insert into session_aggr_stat "
                +"values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
        Object[] param=new Object[]{
                sessionAggrStat.getTaskid(),
                sessionAggrStat.getSession_count(),
                sessionAggrStat.getVisit_length_1s_3s_ratio(),
                sessionAggrStat.getVisit_length_4s_6s_ratio(),
                sessionAggrStat.getVisit_length_7s_9s_ratio(),
                sessionAggrStat.getVisit_length_10s_30s_ratio(),
                sessionAggrStat.getVisit_length_30s_60s_ratio(),
                sessionAggrStat.getVisit_length_1m_3m_ratio(),
                sessionAggrStat.getVisit_length_3m_10m_ratio(),
                sessionAggrStat.getVisit_length_10m_30m_ratio(),
                sessionAggrStat.getVisit_length_30m_ratio(),
                sessionAggrStat.getStep_length_1_3_ratio(),
                sessionAggrStat.getStep_length_4_6_ratio(),
                sessionAggrStat.getStep_length_7_9_ratio(),
                sessionAggrStat.getStep_length_10_30_ratio(),
                sessionAggrStat.getStep_length_30_60_ratio(),
                sessionAggrStat.getStep_length_60_ratio()
        };
        instance.executeUpdate(sql,param);

    }
}
