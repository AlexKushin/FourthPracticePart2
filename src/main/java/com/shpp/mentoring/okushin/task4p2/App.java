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


public class App {
    private static final Logger logger = LoggerFactory.getLogger(App.class);


    public static void main(String[] args) {
        StopWatch watch = new StopWatch();
        CqlExecutor cqlExecutor = new CqlExecutor();
        CsvImporter csvImporter = new CsvImporter();

        Properties prop = new Properties();
        PropertyManager.readPropertyFile("prop.properties", prop);
        logger.info("Property file was successfully read");

        int numberGenerateThreads = PropertyManager.getIntPropertiesValue("numberGenerateThreads", prop);
        int numberDeliveryThreads = PropertyManager.getIntPropertiesValue("numberDeliveryThreads", prop);
        int amountProducts = PropertyManager.getIntPropertiesValue("amountProducts", prop);
        logger.info("All necessary data were successfully read from property file");


        DriverConfigLoader loader = DriverConfigLoader.fromClasspath("application.conf");
        try (CqlSession session = CqlSession.builder().withConfigLoader(loader).build()) {
            logger.info("CqlSession was successfully set up");
            try (ValidatorFactory factory = Validation.buildDefaultValidatorFactory()) {
                //add batch
                cqlExecutor.executeCqlScript(session, "deleteTableIfExistsScript.cql");
                Thread.sleep(20000);
                cqlExecutor.executeCqlScript(session, "DdlScripForCreatingTables.cql");
                Thread.sleep(20000);


                csvImporter.importToDB(session, "stores.csv", "\"epicentrRepo\".stores");
                csvImporter.importToDB(session, "types.csv", "\"epicentrRepo\".productTypes");

                SimpleStatement countTypesStatement =
                        SimpleStatement.newInstance("Select * from \"epicentrRepo\".productTypes");
                SimpleStatement countStoresStatement =
                        SimpleStatement.newInstance("Select * from \"epicentrRepo\".stores");

                int typesCount = session.execute(countTypesStatement).all().size();
                int storesCount = session.execute(countStoresStatement).all().size();

                Validator validator = factory.getValidator();
                logger.info("Validator instance created");
                watch.start();

                Generate generate = new Generate(session, numberGenerateThreads, typesCount, validator, amountProducts);
                generate.createProducts();
                while (true) {
                    if (generate.isGeneratingFinished()) {
                        watch.stop();
                        double generatingTime = watch.getTime() / 1000.0;
                        double productsPerSecond = amountProducts / generatingTime;
                        watch.reset();
                        Delivery delivery = new Delivery(session, numberDeliveryThreads, storesCount, cqlExecutor);
                        watch.start();
                        delivery.deliverToStore();
                        while (true) {
                            if (delivery.isDeliveryFinished()) {
                                logger.info("Delivery has finished successfully");
                                watch.stop();
                                double deliveryTime = watch.getTime() / 1000.0;
                                double deliveriesPerSecond = (amountProducts * numberDeliveryThreads) / deliveryTime;
                                watch.reset();
                                String cqlForNumberProductsInStoreByType = "SELECT * FROM \"epicentrRepo\"." +
                                        "delivery WHERE type = ? and store = ? ";

                                String cqlForInsertAvailability = "insert into \"epicentrRepo\"." +
                                        "availability (type, store, quantity) values (?,?,?) if not exists";

                                PreparedStatement statementForIA = session.prepare(cqlForInsertAvailability);
                                watch.start();
                                for (int typeId = 0; typeId < typesCount; typeId++) {
                                    for (int storeId = 0; storeId < storesCount; storeId++) {
                                        ResultSet resultSet = cqlExecutor.executeCqlPreparedStatement(session,
                                                cqlForNumberProductsInStoreByType, typeId, storeId);

                                        BoundStatement boundForIA = statementForIA.bind(typeId, storeId, resultSet.all().size())
                                                .setConsistencyLevel(DefaultConsistencyLevel.LOCAL_QUORUM);
                                        session.execute(boundForIA);
                                        cqlExecutor.executeCqlPreparedStatement(session,
                                                cqlForInsertAvailability, typeId, storeId, resultSet.all().size());
                                    }
                                }
                                double filingAvailableTime = watch.getTime() / 1000.0;
                                watch.reset();
                                String productType = System.getProperty("productType");


                                Row resRowTypeID = cqlExecutor.executeCqlPreparedStatement(session,
                                        "select id from \"epicentrRepo\".producttypes where producttype = ?",
                                        productType).one();
                                assert resRowTypeID != null;
                                watch.start();
                                Row resStoreId = cqlExecutor.executeCqlPreparedStatement(session,
                                        "select store\n" +
                                                "from \"epicentrRepo\".availability\n" +
                                                "where type = ?\n" +
                                                "order by quantity desc",
                                        resRowTypeID.getInt(0)).one();

                                watch.stop();
                                assert resStoreId != null;
                                Row resRowStoreAddress = cqlExecutor.executeCqlPreparedStatement(session,
                                        "SELECT storeaddress FROM \"epicentrRepo\".stores WHERE id =?",
                                        resStoreId.getInt(0)).one();

                                double searchStoreIdTime = watch.getTime() / 1000.0;

                                logger.info("                                                           ");
                                logger.info("************************************************************");
                                assert resRowStoreAddress != null;
                                logger.info("GENERATING SPEED by {} threads: {} , total = {} products, elapseSeconds = {}"
                                        , numberGenerateThreads, productsPerSecond, amountProducts, generatingTime);
                                logger.info("DELIVERY SPEED by {} threads: {} , total = {} products, elapseSeconds = {}"
                                        , numberDeliveryThreads, deliveriesPerSecond,
                                        amountProducts * numberDeliveryThreads, deliveryTime);
                                logger.info("FILLING AVAILABLE SPEED: {} ", filingAvailableTime);
                                logger.info("PRODUCT TYPE FOR SEARCH BY MAX AMOUNT IN STORE : {}", productType);
                                logger.info("SEARCH STORE TIME: {}s", searchStoreIdTime);
                                logger.info("MAX AMOUNT OF TYPE: {}, IN STORE {}", productType, resRowStoreAddress.getString(0));
                                logger.info("************************************************************");
                                logger.info("                                                           ");
                                return;
                            }
                        }
                    }
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }
}

