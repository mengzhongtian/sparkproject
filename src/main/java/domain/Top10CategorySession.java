package domain;

public class Top10CategorySession {
    private Long taskid;
    private Long categoryid;
    private String sessionid;
    private String clickcount;

    public Top10CategorySession(Long taskid, Long categoryid, String sessionid, String clickcount) {

        this.taskid = taskid;
        this.categoryid = categoryid;
        this.sessionid = sessionid;
        this.clickcount = clickcount;
    }

    public Long getTaskid() {
        return taskid;
    }

    public void setTaskid(Long taskid) {
        this.taskid = taskid;
    }

    public Long getCategoryid() {
        return categoryid;
    }

    public void setCategoryid(Long categoryid) {
        this.categoryid = categoryid;
    }

    public String getSessionid() {
        return sessionid;
    }

    public void setSessionid(String sessionid) {
        this.sessionid = sessionid;
    }

    public String getClickcount() {
        return clickcount;
    }

    public void setClickcount(String clickcount) {
        this.clickcount = clickcount;
    }
}
