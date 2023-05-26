package com.shpp.mentoring.okushin.task4p2;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.cql.*;

import java.util.List;

public class CqlExecutor {

    public  void executeBatch(CqlSession session, List<BatchableStatement<?>> statementList){
        BatchStatement batch = BatchStatement.builder(BatchType.UNLOGGED)
                .addStatements(statementList)
                .setConsistencyLevel(DefaultConsistencyLevel.LOCAL_QUORUM)
                .build();
        session.execute(batch);

    }
    public ResultSet  executeCqlPreparedStatement(CqlSession session, String cqlQuery, Object p1){
        PreparedStatement statement = session.prepare(cqlQuery);
        BoundStatement bound = statement.bind(p1).setConsistencyLevel(DefaultConsistencyLevel.LOCAL_QUORUM);
        return session.execute(bound);
    }
    public ResultSet  executeCqlPreparedStatement(CqlSession session, String cqlQuery, Object p1, Object p2){
        PreparedStatement statement = session.prepare(cqlQuery);
        BoundStatement bound = statement.bind(p1,p2).setConsistencyLevel(DefaultConsistencyLevel.LOCAL_QUORUM);
        return session.execute(bound);
    }
    public ResultSet  executeCqlPreparedStatement(CqlSession session, String cqlQuery, Object p1, Object p2, Object p3){
        PreparedStatement statement = session.prepare(cqlQuery);
        BoundStatement bound = statement.bind(p1,p2,p3).setConsistencyLevel(DefaultConsistencyLevel.LOCAL_QUORUM);
        return session.execute(bound);
    }
    public ResultSet  executeCqlPreparedStatement(CqlSession session, String cqlQuery, Object p1, Object p2, Object p3, Object p4){
        PreparedStatement statement = session.prepare(cqlQuery);
        BoundStatement bound = statement.bind(p1,p2,p3,p4).setConsistencyLevel(DefaultConsistencyLevel.LOCAL_QUORUM);
        return session.execute(bound);
    }
}
