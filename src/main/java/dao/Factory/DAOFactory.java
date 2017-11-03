package dao.Factory;

import dao.SessionAggrDao;
import dao.SessionDetailDao;
import dao.SessionRandomExtractDao;
import dao.TaskDao;
import dao.impl.SessionAggrDaoImpl;
import dao.impl.SessionDetailDaoImpl;
import dao.impl.SessionRandomExtractDaoImpl;
import dao.impl.TaskDaoImpl;
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
}
