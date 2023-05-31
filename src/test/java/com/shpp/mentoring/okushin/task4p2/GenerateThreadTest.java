package com.shpp.mentoring.okushin.task4p2;

import com.datastax.oss.driver.api.core.CqlSession;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

class GenerateThreadTest extends GenerateThread {

    @Mock
    CqlSession sessionMock = mock(CqlSession.class);
    ProductGenerator productGenerator = mock(ProductGenerator.class);
    CqlExecutor cqlExecutor = mock(CqlExecutor.class);

    @Test
    void testRun() {
        GenerateThread generateThread = new GenerateThread(sessionMock,productGenerator,10,10);
        generateThread.run();
        Mockito.verify(productGenerator).insertValidatedProducts(sessionMock,10,10);
    }
}