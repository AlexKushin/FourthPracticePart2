package com.shpp.mentoring.okushin.task4p2;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.cql.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class CqlExecutorTest extends CqlExecutor {


    @Mock
    private CqlSession session;

    @Mock
    private PreparedStatement preparedStatement;

    @Mock
    private BoundStatement boundStatement;

    @Mock
    private ResultSet resultSet;
    @Mock
    SimpleStatement simpleStatementMock;
    @InjectMocks
    private CqlExecutor cqlExecutor;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
    }


    @Test
    void testExecuteCqlPreparedStatement() {
        String cqlQuery = "SELECT * FROM my_table";
        Object p1 = "parameter";

        when(session.prepare(cqlQuery)).thenReturn(preparedStatement);
        when(preparedStatement.bind(p1)).thenReturn(boundStatement);
        when(boundStatement.setConsistencyLevel(DefaultConsistencyLevel.LOCAL_QUORUM)).thenReturn(boundStatement);
        when(session.execute(boundStatement)).thenReturn(resultSet);

        ResultSet result = cqlExecutor.executeCqlPreparedStatement(session, cqlQuery, p1);

        verify(session).prepare(cqlQuery);
        verify(preparedStatement).bind(p1);
        verify(boundStatement).setConsistencyLevel(DefaultConsistencyLevel.LOCAL_QUORUM);
        verify(session).execute(boundStatement);
        assertEquals(resultSet, result);
    }
    @Test
    void testExecuteCqlScript(){

        try (MockedStatic <SimpleStatement> mocked = mockStatic(SimpleStatement.class)) {
            when(SimpleStatement.newInstance(anyString())).thenReturn(simpleStatementMock);
            when(simpleStatementMock.setConsistencyLevel(DefaultConsistencyLevel.LOCAL_QUORUM))
                    .thenReturn(simpleStatementMock);
            CqlExecutor executor = new CqlExecutor();
            executor.executeCqlScript(session,"src/test/resources/test.cql");
            verify(session, times(5)).execute(any(SimpleStatement.class));

        }
    }
}