package dao.Factory;

import dao.*;
import dao.impl.*;
import domain.SessionDetail;
import domain.SessionRandomExtract;

public class DAOFactory {
    public static TaskDao getTask() {
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
    public static Top10CategoryDAO getTop10CategoryDAO(){
        return new Top10CategoryDaoImpl();
    }
}
