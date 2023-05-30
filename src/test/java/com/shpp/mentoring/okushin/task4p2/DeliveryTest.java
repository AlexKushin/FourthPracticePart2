package com.shpp.mentoring.okushin.task4p2;

import com.datastax.oss.driver.api.core.CqlSession;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;

class DeliveryTest extends Delivery {
    @Mock
    CqlExecutor cqlExecutorMock = mock(CqlExecutor.class);
    @Mock
    CqlSession sessionMock = mock(CqlSession.class);



    @Test
    void testDeliverToStore() {


    }
}