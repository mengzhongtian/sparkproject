import dao.Factory.DAOFactory;
import dao.SessionRandomExtractDao;
import domain.SessionRandomExtract;

public class SessionRandomExtractTest {
    public static void main(String[] args) {
        SessionRandomExtractDao sessionRandomExtractDao = DAOFactory.getSessionRandomExtractDao();
        SessionRandomExtract sessionRandomExtract = new SessionRandomExtract();
        sessionRandomExtract.setTask_id(1);
        sessionRandomExtract.setClick_categoryids("2");
        sessionRandomExtract.setSearch_keywords("3");
        sessionRandomExtract.setSession_id("4");
        sessionRandomExtract.setStart_time("8");
        sessionRandomExtractDao.insert(sessionRandomExtract);
    }
}
