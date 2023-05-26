package com.shpp.mentoring.okushin.task4p2;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.cql.*;
import com.shpp.mentoring.okushin.task3.PropertyManager;
import org.apache.commons.lang3.time.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.Validation;
import javax.validation.Validator;
import javax.validation.ValidatorFactory;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class App {
    private static final Logger logger = LoggerFactory.getLogger(App.class);


    public static void main(String[] args) {
       /* DriverConfigLoader loader = DriverConfigLoader.fromClasspath("application.conf");
        try (CqlSession session = CqlSession.builder().withConfigLoader(loader).build()) {
            String cqlForInsertAvailability = "insert into \"epicentrRepo\"." +
                    "availability (type, store, quantity) values (?,?,?) if not exists";
            CqlExecutor cqlExecutor = new CqlExecutor();
            cqlExecutor.executeCqlPreparedStatement(session,cqlForInsertAvailability,44,44,44 );
        }

        */
        //Use DriverConfigLoader to load your configuration file
        StopWatch watch = new StopWatch();
        CqlExecutor cqlExecutor = new CqlExecutor();
        Properties prop = new Properties();
        PropertyManager.readPropertyFile("prop.properties", prop);
        int numberGenerateThreads = PropertyManager.getIntPropertiesValue("numberGenerateThreads",prop);
        int numberDeliveryThreads = PropertyManager.getIntPropertiesValue("numberDeliveryThreads",prop);
        int amountProducts = PropertyManager.getIntPropertiesValue("amountProducts",prop);
        int amountDelivery = PropertyManager.getIntPropertiesValue("amountDelivery",prop);
        int amountForMainThreads = amountProducts / numberGenerateThreads;

        //int amountForAdditionalThread = amount - amountForMainThreads * (numberThreads - 1);


        DriverConfigLoader loader = DriverConfigLoader.fromClasspath("application.conf");
        ExecutorService executorService = Executors.newFixedThreadPool(numberGenerateThreads);
        try (CqlSession session = CqlSession.builder().withConfigLoader(loader).build()) {
            try (ValidatorFactory factory = Validation.buildDefaultValidatorFactory()) {
                CsvImporter.importToDB(session, "stores.csv", "\"epicentrRepo\".stores");
                CsvImporter.importToDB(session, "types.csv", "\"epicentrRepo\".productTypes");

//cqlExecutor
                SimpleStatement countTypesStatement = SimpleStatement.newInstance("Select * from \"epicentrRepo\".productTypes");
                SimpleStatement countStoresStatement = SimpleStatement.newInstance("Select * from \"epicentrRepo\".stores");

                int typesCount = session.execute(countTypesStatement).all().size();
                int storesCount = session.execute(countStoresStatement).all().size();

                Validator validator = factory.getValidator();
                ProductGenerator productGenerator = new ProductGenerator(validator);
                watch.start();

                for (int i = 0; i < numberGenerateThreads; i++) {
                    executorService.submit(new GenerateThread(session, productGenerator, amountForMainThreads, typesCount));
                }
                executorService.shutdown();
                while (true) {
                    if (executorService.isTerminated()) {
                        watch.stop();
                        logger.info("10 connection for generating products are closed");
                        double elapsedSeconds = watch.getTime() / 1000.0;
                        double productsPerSecond = amountProducts / elapsedSeconds;
                        logger.info("GENERATING SPEED by {} threads: {} , total = {} products, elapseSeconds = {}"
                                , numberGenerateThreads, productsPerSecond, amountProducts, elapsedSeconds);

                        Delivery delivery = new Delivery(session, numberDeliveryThreads, storesCount);
                        delivery.deliverToStore();
//cqlExecutor
                        String cqlForNumberProductsInStoreByType = "SELECT * FROM \"epicentrRepo\"." +
                                "delivery WHERE type = ? and store = ? ";
                        PreparedStatement statementForNP = session.prepare(cqlForNumberProductsInStoreByType);

                        String cqlForInsertAvailability = "insert into \"epicentrRepo\"." +
                                "availability (type, store, quantity) values (?,?,?) if not exists";
                        PreparedStatement statementForIA = session.prepare(cqlForInsertAvailability);

                        for (int typeId = 0; typeId < typesCount; typeId++) {
                            for (int storeId = 0; storeId < storesCount; storeId++) {
                             //   BoundStatement boundForNP = statementForNP.bind(typeId, storeId)
                                        //.setInt(0, typeId)
                                        //.setInt(1, storeId)
                                     //   .setConsistencyLevel(DefaultConsistencyLevel.LOCAL_QUORUM);
                              //  ResultSet resultSet = session.execute(boundForNP);
                                ResultSet resultSet = cqlExecutor.executeCqlPreparedStatement(session,cqlForNumberProductsInStoreByType,typeId,storeId);

                                BoundStatement boundForIA = statementForIA.bind(typeId, storeId, resultSet.all().size())
                                        //.setInt(0, typeId)
                                        //.setInt(1, storeId)
                                        //.setInt(2, resultSet.all().size())
                                        .setConsistencyLevel(DefaultConsistencyLevel.LOCAL_QUORUM);
                                session.execute(boundForIA);
                                cqlExecutor.executeCqlPreparedStatement(session,cqlForInsertAvailability,typeId,storeId,resultSet.all().size());
                            }
                        }
                        String productType = System.getProperty("productType");

//cqlExecutor
                        /*PreparedStatement statementTypeId = session.prepare("select id\n" +
                                "from \"epicentrRepo\".producttypes\n" +
                                "where producttype = ?");
                        BoundStatement boundTypeId = statementTypeId.bind(productType);
                        ResultSet resultSetStoreId = session.execute(boundTypeId);
                        Row resRow = resultSetStoreId.one();

                         */
                        Row resRow = cqlExecutor.executeCqlPreparedStatement(session,
                                "select id from \"epicentrRepo\".producttypes where producttype = ?", productType).one();


                        /*PreparedStatement statement = session.prepare("SELECT storeaddress FROM \"epicentrRepo\".stores WHERE id =?");
                        assert resRow != null;
                        BoundStatement bound = statement.bind(resRow.getInt(0));
                        ResultSet resAddress = session.execute(bound);
                        Row res = resAddress.one();

                         */
                        Row res = cqlExecutor.executeCqlPreparedStatement(session,"SELECT storeaddress FROM \"epicentrRepo\".stores WHERE id =?",resRow.getInt(0)).one();

                        logger.info("                                                           ");
                        logger.info("************************************************************");
                        assert res != null;
                        logger.info("necessary storeId: {}", res.getString(0));
                        logger.info("************************************************************");
                        logger.info("                                                           ");
                        return;
                    }
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }


    }
}

