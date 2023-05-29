package com.shpp.mentoring.okushin.task4p2;

import com.datastax.oss.driver.api.core.CqlSession;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;

import static org.mockito.Mockito.mock;

class CsvImporterTest extends CsvImporter {

    CsvImporter csvImporterMock = mock(CsvImporter.class);
    CqlSession sessionMock = mock(CqlSession.class);

    @Test
    void testImportToDB() throws IOException, InterruptedException {
        csvImporterMock.importToDB(sessionMock, "csvFile", "tableName");
        Mockito.verify(csvImporterMock).importToDB(sessionMock, "csvFile", "tableName");
    }
}