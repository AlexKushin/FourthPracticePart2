package com.shpp.mentoring.okushin.task4p2;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BatchableStatement;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.Validator;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

public class ProductGenerator {
    private static final Logger logger = LoggerFactory.getLogger(ProductGenerator.class);
    private final Validator validator;
    Random random = new Random();


    public ProductGenerator(Validator validator) {
        logger.info("ProductGenerator instance created");
        this.validator = validator;

    }


    public void insertValidatedProducts(CqlSession session, int amount, int typesCount) {

        final AtomicInteger totalQuantity = new AtomicInteger(0);
        int batchCount = 0;
        int batchSize = 30;
        logger.debug("Batch size  = {}", batchSize);
        StopWatch watch = new StopWatch();
        CqlExecutor cqlExecutor = new CqlExecutor();
        watch.start();
        String cql = "INSERT INTO \"epicentrRepo\".\"products\" (id,type,name) VALUES" + " (now(), ?, ? ) IF NOT EXISTS ";
        logger.debug("----------------------------------------");
        logger.debug("CQL command for insert to Products table: {}",cql);
        logger.debug("----------------------------------------");
        int leftAmount = amount;

        List<BatchableStatement<?>> statementList = new ArrayList<>();
        PreparedStatement statement = session.prepare(cql);

        while (leftAmount > 0) {
            Product p = new Product(RandomStringUtils.randomAlphabetic(10),
                    random.nextInt(typesCount + 1), random.nextInt(120) );
            if (validator.validate(p).isEmpty()) {
                BoundStatement bound = statement.bind( p.getTypeId(),p.getName());

                // Consistency level LOCAL_ONE is not supported for this operation.
                // Supported consistency levels are: LOCAL_QUORUM
                //https://docs.aws.amazon.com/keyspaces/latest/devguide/consistency.html

                statementList.add(bound);
                batchCount++;

                if (batchCount > 0 && batchCount % batchSize == 0) {
                    cqlExecutor.executeBatch(session, statementList);
                    statementList = new ArrayList<>();
                }

                totalQuantity.incrementAndGet();
                leftAmount--;
            }
        }
        cqlExecutor.executeBatch(session, statementList);
        watch.stop();
        double elapsedSeconds = watch.getTime() / 1000.0;
        double messagesPerSecond = totalQuantity.get() / elapsedSeconds;
        logger.info("batchSize = {}", batchSize);
        logger.info("GENERATING SPEED: {} , total = {} products, elapseSeconds = {}",
                messagesPerSecond, totalQuantity.get(), elapsedSeconds);

    }
}
