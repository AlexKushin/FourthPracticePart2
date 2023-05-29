package com.shpp.mentoring.okushin.task4p2;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;
import com.shpp.mentoring.okushin.exceptions.ReadFileException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

public class CsvImporter {

    private static final Logger logger = LoggerFactory.getLogger(CsvImporter.class);

    public  void importToDB(CqlSession session, String csvFilePath, String tableName) {

        try (CSVReader reader = new CSVReader(new FileReader(csvFilePath))) {
            String[] nextLine;
            String[] header = reader.readNext();
            StringBuilder cql = new StringBuilder("INSERT INTO ");
            cql.append(tableName);
            cql.append("(");
            for (int i = 0; i < header.length; i++) {
                cql.append(header[i]);
                if (i != header.length - 1) {
                    cql.append(",");
                }
            }
            cql.append(") VALUES (");
            for (int i = 0; i < header.length; i++) {
                cql.append("?");
                if (i != header.length - 1) {
                    cql.append(",");
                }
            }
            cql.append(")");

            logger.info("----------------------------------------");
            logger.info("CQL command for import from csv: {}",cql);
            logger.info("----------------------------------------");

            PreparedStatement statement = session.prepare(String.valueOf(cql));


            while ((nextLine = reader.readNext()) != null) {
                BoundStatement bound = statement.bind()
                        .setInt(0, Integer.parseInt(nextLine[0]))
                        .setString(1, nextLine[1])
                        .setConsistencyLevel(DefaultConsistencyLevel.LOCAL_QUORUM);
                session.execute(bound);

            }
        }catch (CsvValidationException e) {
            logger.error("Error while validation CSV file: {} ", e.getMessage());
        } catch (FileNotFoundException e) {
            logger.error("There is no file to read {}", e.getMessage());
        } catch (IOException e) {
            logger.error("Error while input/output {}", e.getMessage());
            throw new ReadFileException("Can't read file by path");
        }
    }
}
