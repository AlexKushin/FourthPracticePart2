package com.shpp.mentoring.okushin.task4p2;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.cql.*;
import com.shpp.mentoring.okushin.exceptions.ReadFileException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;

public class CqlExecutor {
    private static final Logger logger = LoggerFactory.getLogger(CqlExecutor.class);

    public  void executeBatch(CqlSession session, List<BatchableStatement<?>> statementList){
        logger.debug("batchType: {}, DefaultConsistencyLevel: {}",
                BatchType.UNLOGGED, DefaultConsistencyLevel.LOCAL_QUORUM);
        BatchStatement batch = BatchStatement.builder(BatchType.UNLOGGED)
                .addStatements(statementList)
                .setConsistencyLevel(DefaultConsistencyLevel.LOCAL_QUORUM)
                .build();
        session.execute(batch);
    }
    public void executeCqlSimpleStatement(CqlSession session, String cqlQuery){
        SimpleStatement statement = SimpleStatement.newInstance(cqlQuery)
                        .setConsistencyLevel(DefaultConsistencyLevel.LOCAL_QUORUM);
        session.execute(statement);
    }
    public ResultSet  executeCqlPreparedStatement(CqlSession session, String cqlQuery, Object p1){
        PreparedStatement statement = session.prepare(cqlQuery);
        BoundStatement bound = statement.bind(p1).setConsistencyLevel(DefaultConsistencyLevel.LOCAL_QUORUM);
        return session.execute(bound);
    }
    public ResultSet  executeCqlPreparedStatement(CqlSession session, String cqlQuery, Object p1, Object p2){

        PreparedStatement statement = session.prepare(cqlQuery);
        BoundStatement bound = statement.bind(p1,p2).setConsistencyLevel(DefaultConsistencyLevel.LOCAL_QUORUM);
        return session.execute(bound);
    }
    public ResultSet  executeCqlPreparedStatement(CqlSession session, String cqlQuery, Object p1, Object p2, Object p3){
        PreparedStatement statement = session.prepare(cqlQuery);
        BoundStatement bound = statement.bind(p1,p2,p3).setConsistencyLevel(DefaultConsistencyLevel.LOCAL_QUORUM);
        return session.execute(bound);
    }
    public ResultSet  executeCqlPreparedStatement(CqlSession session, String cqlQuery, Object p1, Object p2, Object p3, Object p4){
        PreparedStatement statement = session.prepare(cqlQuery);
        BoundStatement bound = statement.bind(p1,p2,p3,p4).setConsistencyLevel(DefaultConsistencyLevel.LOCAL_QUORUM);
        return session.execute(bound);
    }
    public void executeCqlScript(CqlSession session, String sqlFilePath) {
            String[] commands = readCqlScriptFromFile(sqlFilePath).split(";");
            for (String command : commands) {
                if(!command.isBlank()) {
                    SimpleStatement statement = SimpleStatement.newInstance(command)
                            .setConsistencyLevel(DefaultConsistencyLevel.LOCAL_QUORUM);
                    session.execute(statement);
                }
        }
    }
    private String readCqlScriptFromFile(String cqlFilePath) {
        try (BufferedReader br = new BufferedReader(new FileReader(cqlFilePath))) {
            String line;
            StringBuilder sb = new StringBuilder();
            while ((line = br.readLine()) != null) {
                sb.append(line);
                sb.append(" ");
            }
            String cqlScript = sb.toString();
            logger.debug("                                                    ");
            logger.debug("****************************************************");
            logger.debug("CQL SCRIPT:  {}", cqlScript);
            logger.debug("****************************************************");
            logger.debug("                                                    ");
            return cqlScript;
        } catch (IOException e) {
            logger.error("Error while reading file: {}", e.getMessage(), e);
            throw new ReadFileException("Can't read file by path");
        }
    }
}
