package com.shpp.mentoring.okushin.task4p2;

import com.datastax.oss.driver.api.core.CqlSession;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;

class CsvImporterTest extends CsvImporter {

    CsvImporter csvImporterMock = mock(CsvImporter.class);
    CqlSession sessionMock = mock(CqlSession.class);

    @Test
    void testImportToDB() throws IOException, InterruptedException {
        csvImporterMock.importToDB(sessionMock, "csvFile", "tableName");
        csvImporterMock.importToDB(sessionMock, "csvFile", "tableName");
        Mockito.verify(csvImporterMock, times(2)).importToDB(sessionMock, "csvFile", "tableName");
    }
}