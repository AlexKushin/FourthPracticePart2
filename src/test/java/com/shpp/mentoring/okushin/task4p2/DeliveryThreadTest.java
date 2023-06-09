package com.shpp.mentoring.okushin.task4p2;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.cql.*;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.when;

class DeliveryThreadTest extends DeliveryThread {

    @Mock
    CqlSession sessionMock = mock(CqlSession.class);
    @Mock
    private ResultSet resultSet = mock(ResultSet.class);
    @Mock
    private PreparedStatement preparedStatement = mock(PreparedStatement.class);

    @Mock
    private BoundStatement boundStatement = mock(BoundStatement.class);
    @Mock
    private Row rowMock = mock(Row.class);


    @Test
    void testRun() {
        /*List<Row> listRow = new ArrayList<>();
        listRow.add(rowMock);

        when(sessionMock.prepare(anyString())).thenReturn(preparedStatement);
        when(preparedStatement.bind(any())).thenReturn(boundStatement);
        when(boundStatement.setConsistencyLevel(DefaultConsistencyLevel.LOCAL_QUORUM)).thenReturn(boundStatement);
        when(sessionMock.execute(boundStatement)).thenReturn(resultSet);

        DeliveryThread deliveryThread = new DeliveryThread(listRow, sessionMock, 10,10);
        deliveryThread.run();

        Mockito.verify(sessionMock).execute(any(BoundStatement.class));

         */
    }
}