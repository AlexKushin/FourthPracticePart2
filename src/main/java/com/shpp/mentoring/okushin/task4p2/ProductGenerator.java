package com.shpp.mentoring.okushin.task4p2;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.cql.*;
import com.datastax.oss.driver.api.core.metadata.token.Token;
import com.datastax.oss.driver.internal.core.metadata.token.Murmur3TokenFactory;
import com.datastax.oss.driver.internal.core.metadata.token.TokenFactory;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.Validator;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

public class ProductGenerator {
    private static final Logger logger = LoggerFactory.getLogger(ProductGenerator.class);
    private final Validator validator;
    Random random = new Random();


    public ProductGenerator(Validator validator) {
        this.validator = validator;

    }


    public void insertValidatedProducts(CqlSession session, int amount, int typesCount, int storesCount) {

        final AtomicInteger totalQuantity = new AtomicInteger(0);
        int batchCount = 0;
        BatchStatement batch;

        int batchSize = 30;
        StopWatch watch = new StopWatch();
        watch.start();
        String cql = "INSERT INTO \"epicentrRepo\".\"products\" (id,type,name, store) VALUES" + " (?, ?, ?, ? ) IF NOT EXISTS ";
        int leftAmount = amount;

       List<BatchableStatement<?>> statementList = new ArrayList<>();

        TokenFactory tokenFactory = new Murmur3TokenFactory();
        PreparedStatement statement = session.prepare(cql);

        while (leftAmount > 0) {
            Product p = new Product(RandomStringUtils.randomAlphabetic(10), random.nextInt(typesCount + 1));
            if (validator.validate(p).isEmpty()) {
                byte[] bytes2 = p.getName().getBytes(StandardCharsets.UTF_8);
                ByteBuffer buffer2 = ByteBuffer.wrap(bytes2);
                Token token = tokenFactory.hash(buffer2);

                BoundStatement bound = statement.bind()
                        .setInt(0, token.hashCode())
                        .setInt(1, p.getTypeId())
                        .setString(2, p.getName())
                        .setInt(3, random.nextInt(storesCount+1))
                        // Consistency level LOCAL_ONE is not supported for this operation.
                        // Supported consistency levels are: LOCAL_QUORUM
                        //https://docs.aws.amazon.com/keyspaces/latest/devguide/consistency.html

                        ;
              // session.execute(bound);
                statementList.add(bound);
                batchCount++;

                if (batchCount > 0 && batchCount % batchSize == 0) {
                    batch = BatchStatement.builder(BatchType.UNLOGGED)
                            .addStatements(statementList)
                            .setConsistencyLevel(DefaultConsistencyLevel.LOCAL_QUORUM)
                            .build();
                    session.execute(batch);
                    statementList = new ArrayList<>();
                }

                totalQuantity.incrementAndGet();
                leftAmount--;
            }

        }


       batch = BatchStatement.builder(BatchType.UNLOGGED)
                .addStatements(statementList)
                .setConsistencyLevel(DefaultConsistencyLevel.LOCAL_QUORUM)
                .build();
        session.execute(batch);
        watch.stop();
        double elapsedSeconds = watch.getTime() / 1000.0;
        double messagesPerSecond = totalQuantity.get() / elapsedSeconds;
        logger.info("batchSize = {}", batchSize);
        logger.info("GENERATING SPEED: {} , total = {} messages, elapseSeconds = {}",
                messagesPerSecond, totalQuantity.get(), elapsedSeconds);

    }
}
