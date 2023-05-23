package com.shpp.mentoring.okushin.task4p2;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import org.apache.commons.lang3.time.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.Validation;
import javax.validation.Validator;
import javax.validation.ValidatorFactory;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class App {
    private static final Logger logger = LoggerFactory.getLogger(App.class);

    public static void main(String[] args) {

        //Use DriverConfigLoader to load your configuration file
        StopWatch watch = new StopWatch();
        DriverConfigLoader loader = DriverConfigLoader.fromClasspath("application.conf");
        int numberThreads = 10;
        ExecutorService executorService = Executors.newFixedThreadPool(numberThreads);


        int amount = 1000;
        int amountForMainThreads = amount / numberThreads;
        //int amountForAdditionalThread = amount - amountForMainThreads * (numberThreads - 1);


        try (CqlSession session = CqlSession.builder().withConfigLoader(loader).build()) {
            try (ValidatorFactory factory = Validation.buildDefaultValidatorFactory()) {
                CsvImporter.importToDB(session, "stores.csv", "\"epicentrRepo\".stores");
                CsvImporter.importToDB(session, "types.csv", "\"epicentrRepo\".productTypes");

                SimpleStatement countTypesStatement = SimpleStatement.newInstance("Select * from \"epicentrRepo\".productTypes");
                SimpleStatement countStoresStatement = SimpleStatement.newInstance("Select * from \"epicentrRepo\".stores");

                int typesCount = session.execute(countTypesStatement).all().size();
                int storesCount = session.execute(countStoresStatement).all().size();

                Validator validator = factory.getValidator();
                ProductGenerator productGenerator = new ProductGenerator(validator);
                watch.start();

                for (int i = 0; i < numberThreads; i++) {
                    executorService.submit(new GenerateThread(session, productGenerator, amountForMainThreads, typesCount,storesCount));
                }

                executorService.shutdown();
                while (true) {
                    if (executorService.isTerminated()) {
                        watch.stop();
                        logger.info("10 connection for generating products are closed");
                        double elapsedSeconds = watch.getTime() / 1000.0;
                        double productsPerSecond = amount / elapsedSeconds;
                        logger.info("GENERATING SPEED by {} threads: {} , total = {} products, elapseSeconds = {}"
                                , numberThreads, productsPerSecond, amount, elapsedSeconds);
                        String cqlForNumberProductsInStoreByType = "SELECT * FROM \"epicentrRepo\"." +
                                "products WHERE type = ? and store = ? ";
                        PreparedStatement statementForNP = session.prepare(cqlForNumberProductsInStoreByType);
                        String cqlForInsertAvailability = "insert into \"epicentrRepo\"." +
                                "availability (type, store, quantity) values (?,?,?) if not exists";
                        PreparedStatement statementForIA = session.prepare(cqlForInsertAvailability);


                        for (int typeId = 0; typeId < typesCount; typeId++) {
                            for (int storeId = 0; storeId < storesCount; storeId++) {
                                BoundStatement boundForNP = statementForNP.bind()
                                        .setInt(0, typeId)
                                        .setInt(1, storeId)
                                        .setConsistencyLevel(DefaultConsistencyLevel.LOCAL_QUORUM);

                                ResultSet resultSet = session.execute(boundForNP);

                                BoundStatement boundForIA = statementForIA.bind()
                                        .setInt(0, typeId)
                                        .setInt(1, storeId)
                                        .setInt(2, resultSet.all().size())
                                        .setConsistencyLevel(DefaultConsistencyLevel.LOCAL_QUORUM);
                                session.execute(boundForIA);
                            }
                        }
                        SimpleStatement searchStoreStatement = SimpleStatement.newInstance("select store\n" +
                                "from \"epicentrRepo\".availability\n" +
                                "where type = 2\n" +
                                "order by quantity desc\n" +
                                "limit 1");

                        ResultSet res = session.execute(searchStoreStatement);
                        logger.info("necessary storeId: {}",res.one());
                        return;
                    }
                }
            }
        }
    }
}

