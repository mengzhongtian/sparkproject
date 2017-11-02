package dao.impl;

import dao.TaskDao;
import domain.Task;
import jdbc.JDBCHelper;

import java.sql.ResultSet;
import java.sql.SQLException;

public class TaskDaoImpl implements TaskDao {


    public Task findById(long taskid) {
        final Task task =new Task();
        JDBCHelper instance = JDBCHelper.getInstance();
        String sql="select * from task where task_id =?";
        Object[] param = new Object[]{taskid};

        instance.executeQuery(sql, param, new JDBCHelper.QueryCallback() {
            public void process(ResultSet rs) throws SQLException {
                if (rs.next()) {

                    task.setTask_id(rs.getLong(1));
                    task.setTask_name(rs.getString(2));
                    task.setCreate_time(rs.getString(3));
                    task.setStart_time(rs.getString(4));
                    task.setFinish_time(rs.getString(5));
                    task.setTask_type(rs.getString(6));
                    task.setTask_status(rs.getString(7));
                    task.setTask_param(rs.getString(8));

                }
            }
        });


        return task;
    }
}
