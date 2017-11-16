package dao.Factory;

import dao.*;
import dao.impl.*;

public class DAOFactory {
    public static TaskDao getTaskDao() {
        return new TaskDaoImpl();
    }

    public static SessionAggrDao getSessionAggrDao() {
        return new SessionAggrDaoImpl();
    }

    public static SessionRandomExtractDao getSessionRandomExtractDao() {
        return new SessionRandomExtractDaoImpl();
    }

    public static SessionDetailDao getSessionDetailDao() {
        return new SessionDetailDaoImpl();
    }

    public static Top10CategoryDAO getTop10CategoryDAO() {
        return new Top10CategoryDaoImpl();
    }

    public static Top10CategorySessionDAO getTop10CategorySessionDAO() {
        return new Top10CategorySessionDAOImpl();
    }

    public static PageConvertRateDao getPageConvertRateDao() {
        return new PageConvertRateDaoImpl();
    }
}
