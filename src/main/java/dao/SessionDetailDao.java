package dao;

import domain.SessionDetail;

import java.util.List;

public interface SessionDetailDao {
    void insert(SessionDetail sessionDetail);
    void insertBatch(List<SessionDetail> list);
}
