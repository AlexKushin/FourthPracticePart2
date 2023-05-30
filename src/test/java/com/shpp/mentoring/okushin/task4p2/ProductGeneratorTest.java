package com.shpp.mentoring.okushin.task4p2;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BatchableStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

import javax.validation.Validator;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.Mockito.*;

class ProductGeneratorTest extends ProductGenerator {


    @Mock
    CqlExecutor cqlExecutor = mock(CqlExecutor.class);
    CqlSession sessionMock = spy(CqlSession.class);
    Validator validator = mock(Validator.class);
    List<BatchableStatement<?>> listBatchMock = new ArrayList<>();
    ProductGenerator productGenerator = mock(ProductGenerator.class);
    @Test
    void testInsertValidatedProducts() {

    }
}