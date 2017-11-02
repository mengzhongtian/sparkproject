package dao;

import domain.Task;

public interface TaskDao {
    Task findById(long taskid);
}
