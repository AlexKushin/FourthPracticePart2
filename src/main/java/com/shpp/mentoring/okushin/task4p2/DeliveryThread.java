package com.shpp.mentoring.okushin.task4p2;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BatchableStatement;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class DeliveryThread implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(DeliveryThread.class);
    private  int storesCount;
    Random random;
    List<Row> resRow;
    CqlSession session;
    CqlExecutor cqlExecutor;

    public DeliveryThread(List<Row> resRow, CqlSession session, int storesCount, CqlExecutor cqlExecutor) {

        random = new Random();
        this.resRow = resRow;
        this.session = session;
        this.storesCount = storesCount;
        this.cqlExecutor = cqlExecutor;
    }

    public DeliveryThread() {

    }

    @Override
    public void run() {
        logger.info("DeliveryThread starts");
        int batchCount = 0;
        int batchSize = 30;
        int prodCounter = 0;
        List<BatchableStatement<?>> statementList = new ArrayList<>();
        String cql = "insert into \"epicentrRepo\".delivery (id,deliveryDateTime, type,store, name) values (? ,toUnixTimestamp(now()),?,?,?)";
        PreparedStatement statement = session.prepare(cql);
        for (Row row : resRow) {
            BoundStatement bound = statement.bind(row.getUuid("id"), row.getInt("type"),
                    random.nextInt(storesCount) + 1, row.getString("name"));
            prodCounter++;
            statementList.add(bound);
            batchCount++;
            if (batchCount > 0 && batchCount % batchSize == 0) {
                cqlExecutor.executeBatch(session, statementList);
                statementList = new ArrayList<>();
            }
        }
        cqlExecutor.executeBatch(session, statementList);
        logger.info("                                                         ");
        logger.info("*********************************************************");
        logger.info(" Number of products delivered by thread: {}", prodCounter);
        logger.info("*********************************************************");
        logger.info("                                                         ");
    }
}
