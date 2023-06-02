package com.shpp.mentoring.okushin.task4p2;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.Validation;
import javax.validation.Validator;
import javax.validation.ValidatorFactory;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

public class ProductGenerator {
    private static final Logger logger = LoggerFactory.getLogger(ProductGenerator.class);
    Random random;
    Validator validator;

    public ProductGenerator() {
        logger.info("ProductGenerator instance created");
        random = new Random();
        try (ValidatorFactory factory = Validation.buildDefaultValidatorFactory()) {
            validator = factory.getValidator();
        }
    }


    public void insertValidatedProducts(CqlSession session, int amount, int typesCount) {
        AtomicInteger totalQuantity = new AtomicInteger(0);
        StopWatch watch = new StopWatch();
        watch.start();
        String cql = "INSERT INTO \"epicentrRepo\".\"products\" (id,type,name) VALUES (?, ?, ? ) ";
        logger.debug("----------------------------------------");
        logger.debug("CQL command for insert to Products table: {}", cql);
        logger.debug("----------------------------------------");
        PreparedStatement statement = session.prepare(cql);

//why limit() doesn't work, but takeWhile does????
            Stream.generate(() -> new Product(RandomStringUtils.randomAlphabetic(3, 20),
                            random.nextInt(typesCount + 1)))
                    .takeWhile(n->totalQuantity.get()<amount)
                    .forEach(p -> {
                        if (validator.validate(p).isEmpty()) {
                            UUID id = UUID.randomUUID();
                            BoundStatement bound = statement.bind(id, p.getTypeId(), p.getName())
                                    .setConsistencyLevel(DefaultConsistencyLevel.LOCAL_QUORUM);
                            session.execute(bound);
                            totalQuantity.incrementAndGet();
                        }
                    });



            /*while (totalQuantity.get() < amount) {
                Product p = new Product(RandomStringUtils.randomAlphabetic(3, 20),
                        random.nextInt(typesCount + 1));
                if (validator.validate(p).isEmpty()) {
                    UUID id = UUID.randomUUID();
                    BoundStatement bound = statement.bind(id, p.getTypeId(), p.getName())
                            .setConsistencyLevel(DefaultConsistencyLevel.LOCAL_QUORUM);
                    // Consistency level LOCAL_ONE is not supported for this operation.
                    // Supported consistency levels are: LOCAL_QUORUM
                    //you need to assign consistency level
                    //https://docs.aws.amazon.com/keyspaces/latest/devguide/consistency.html
                    session.execute(bound);
                    totalQuantity.incrementAndGet();
                }
            }

             */
        watch.stop();
        double elapsedSeconds = watch.getTime() / 1000.0;
        double messagesPerSecond = totalQuantity.get() / elapsedSeconds;
        logger.info("GENERATING SPEED: {} , total = {} products, elapseSeconds = {}",
                messagesPerSecond, totalQuantity, elapsedSeconds);
    }
}
