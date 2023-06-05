package com.shpp.mentoring.okushin.task4p2;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.opencsv.CSVReader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.*;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.*;

class CsvImporterTest extends CsvImporter {

    @Mock
    private CqlSession session;

    @Mock
    private PreparedStatement preparedStatement;

    @Mock
    private BoundStatement boundStatement;
    @Mock
    private ResultSet resultSet;
    @InjectMocks
    private CsvImporter csvImporter;


    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    void testImportToDB() {
        try (MockedConstruction<CSVReader> mocked = Mockito.mockConstruction(CSVReader.class,
                (mock, context) -> {
                    when(mock.readNext()).thenReturn(new String[]{"id", "name"})
                            .thenReturn(new String[]{"1", "John"})
                            .thenReturn(new String[]{"1", "John"})
                            .thenReturn(null);
                })) {

            when(session.prepare(anyString())).thenReturn(preparedStatement);
            when(preparedStatement.bind(any())).thenReturn(boundStatement);
            when(boundStatement.setConsistencyLevel(DefaultConsistencyLevel.LOCAL_QUORUM)).thenReturn(boundStatement);
            when(session.execute(boundStatement)).thenReturn(resultSet);

            assertTrue(csvImporter.importToDB(session, "stores.csv", "table_name"));
        }
    }


}