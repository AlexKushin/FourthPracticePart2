package com.shpp.mentoring.okushin.task4p2;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BatchableStatement;
import com.datastax.oss.driver.api.core.cql.Row;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;

class DeliveryThreadTest extends DeliveryThread {
    @Mock
    CqlExecutor cqlExecutorMock = mock(CqlExecutor.class);
    @Mock
    CqlSession sessionMock = mock(CqlSession.class);
    List <Row> listRowMock = new ArrayList<>();

    List <BatchableStatement<?>> listBatchMock = new ArrayList<>();

    @Test
    void testRun() {


        DeliveryThread deliveryThread = new DeliveryThread(listRowMock,sessionMock,10,cqlExecutorMock);
        deliveryThread.run();
        Mockito.verify(cqlExecutorMock).executeBatch(sessionMock,listBatchMock);
    }
}