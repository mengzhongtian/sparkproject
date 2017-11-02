package domain;

public class SessionRandomExtract {
    private long task_id;

    public long getTask_id() {
        return task_id;
    }

    public void setTask_id(long task_id) {
        this.task_id = task_id;
    }

    public String getSession_id() {
        return session_id;
    }

    public void setSession_id(String session_id) {
        this.session_id = session_id;
    }

    public String getStart_time() {
        return start_time;
    }

    public void setStart_time(String start_time) {
        this.start_time = start_time;
    }

    public String getClick_categoryids() {
        return click_categoryids;
    }

    public void setClick_categoryids(String click_categoryids) {
        this.click_categoryids = click_categoryids;
    }

    public String getSearch_keywords() {
        return search_keywords;
    }

    public void setSearch_keywords(String search_keywords) {
        this.search_keywords = search_keywords;
    }

    private String session_id;
    private String start_time;
    private String click_categoryids;
    private String search_keywords;
}
