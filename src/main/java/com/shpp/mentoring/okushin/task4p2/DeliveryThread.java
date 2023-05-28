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
    private final int storesCount;
    Random random;
    List<Row> resRow;
    CqlSession session;

    public DeliveryThread(List<Row> resRow, CqlSession session, int storesCount) {

        random = new Random();
        this.resRow = resRow;
        this.session = session;
        this.storesCount = storesCount;
    }

    @Override
    public void run() {
        logger.info("DeliveryThread starts");
        int batchCount = 0;
        int batchSize = 30;
        CqlExecutor cqlExecutor = new CqlExecutor();
        List<BatchableStatement<?>> statementList = new ArrayList<>();
        String cql = "insert into \"epicentrRepo\".delivery (id,deliveryDateTime, type,store, name) values (? ,toUnixTimestamp(now()),?,?,?)";
        PreparedStatement statement = session.prepare(cql);
        for (Row row : resRow) {
            BoundStatement bound = statement.bind(row.getUuid("id"),row.getInt("type"),
                            random.nextInt(storesCount + 1),row.getString("name"));
            statementList.add(bound);
            batchCount++;
            if (batchCount > 0 && batchCount % batchSize == 0) {
                cqlExecutor.executeBatch(session, statementList);
                statementList = new ArrayList<>();
            }
        }
        cqlExecutor.executeBatch(session, statementList);
    }
}
