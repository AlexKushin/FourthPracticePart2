package com.shpp.mentoring.okushin.task4p2;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Random;

public class DeliveryThread implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(DeliveryThread.class);
    private int storesCount;
    Random random;
    List<Row> resRow;
    CqlSession session;

    public DeliveryThread(List<Row> resRow, CqlSession session, int storesCount) {

        random = new Random();
        this.resRow = resRow;
        this.session = session;
        this.storesCount = storesCount;

    }

    public DeliveryThread() {

    }

    @Override
    public void run() {
        logger.info("DeliveryThread starts");
        int prodCounter = 0;
        String cql = "insert into \"epicentrRepo\".delivery (id,deliveryDateTime, type,store) values (? ,toUnixTimestamp(now()),?,?)";
        PreparedStatement statement = session.prepare(cql);
        logger.debug("                                                    ");
        logger.debug("****************************************************");
        logger.debug("CQL PreparedStatement: {}", cql);
        logger.debug("****************************************************");
        logger.debug("                                                    ");
        for (Row row : resRow) {
            BoundStatement bound = statement.bind(row.getUuid("id"), row.getInt("type"),
                            random.nextInt(storesCount) + 1)
                    .setConsistencyLevel(DefaultConsistencyLevel.LOCAL_QUORUM);
            session.execute(bound);
            prodCounter++;

        }
        logger.info("                                                         ");
        logger.info("*********************************************************");
        logger.info("                                                         ");
        logger.info(" Number of products delivered by thread: {}", prodCounter);
        logger.info("                                                         ");
        logger.info("*********************************************************");
        logger.info("                                                         ");
    }
}
