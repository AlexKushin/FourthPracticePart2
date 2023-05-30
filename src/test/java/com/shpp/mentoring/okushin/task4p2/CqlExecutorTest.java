package com.shpp.mentoring.okushin.task4p2;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchableStatement;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

class CqlExecutorTest extends CqlExecutor {
    @Mock
    CqlExecutor cqlExecutorMock = mock(CqlExecutor.class);
    CqlSession sessionMock = mock(CqlSession.class);
    BatchStatement batch = mock(BatchStatement.class);
    List listMock = mock(List.class);

    @Test
    void testExecuteBatch() {
      cqlExecutorMock.executeBatch(sessionMock,listMock);
        Mockito.verify(cqlExecutorMock).executeBatch(sessionMock,listMock);
        //Mockito.verify(sessionMock).execute(batch);
    }

    @Test
    void testExecuteCqlSimpleStatement() {
        cqlExecutorMock.executeCqlSimpleStatement(sessionMock,"123");
        Mockito.verify(cqlExecutorMock).executeCqlSimpleStatement(sessionMock,"123");
    }

    @Test
    void testExecuteCqlPreparedStatement() {
        cqlExecutorMock.executeCqlPreparedStatement(sessionMock,"123","1");
        Mockito.verify(cqlExecutorMock).executeCqlPreparedStatement(sessionMock,"123","1");
    }

    @Test
    void testExecuteCqlPreparedStatement1() {
        cqlExecutorMock.executeCqlPreparedStatement(sessionMock,"123","1","2");
        Mockito.verify(cqlExecutorMock).executeCqlPreparedStatement(sessionMock,"123","1","2");
    }

    @Test
    void testExecuteCqlPreparedStatement2() {
        cqlExecutorMock.executeCqlPreparedStatement(sessionMock,"123","1","2","3");
        Mockito.verify(cqlExecutorMock).executeCqlPreparedStatement(sessionMock,"123","1","2","3");
    }

    @Test
    void testExecuteCqlPreparedStatement3() {
        cqlExecutorMock.executeCqlPreparedStatement(sessionMock,"123","1","2","3","4");
        Mockito.verify(cqlExecutorMock).executeCqlPreparedStatement(sessionMock,"123","1","2","3","4");
    }

    @Test
    void testExecuteCqlScript() {
        cqlExecutorMock.executeCqlScript(sessionMock,"CqlScript.cql");
        Mockito.verify(cqlExecutorMock).executeCqlScript(sessionMock,"CqlScript.cql");
    }
}