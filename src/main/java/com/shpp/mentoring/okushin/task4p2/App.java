package com.shpp.mentoring.okushin.task4p2;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.cql.*;
import com.shpp.mentoring.okushin.task3.PropertyManager;
import org.apache.commons.lang3.time.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

        int numberGenerateThreads = PropertyManager.getIntPropertiesValue("numberGenerateThreads", prop) + 1;
        int numberDeliveryThreads = PropertyManager.getIntPropertiesValue("numberDeliveryThreads", prop);
        int amountProducts = PropertyManager.getIntPropertiesValue("amountProducts", prop);
        String productType = System.getProperty("productType");
        logger.info("All necessary data were successfully read from property file");

        DriverConfigLoader loader = DriverConfigLoader.fromClasspath("application.conf");
        try (CqlSession session = CqlSession.builder().withConfigLoader(loader).build()) {
            logger.info("CqlSession was successfully set up");
            cqlExecutor.executeCqlScript(session, "deleteTableIfExistsScript.cql");
            Thread.sleep(20000);
            cqlExecutor.executeCqlScript(session, "DdlScripForCreatingTables.cql");
            Thread.sleep(20000);
            logger.info("Tables were successfully created");
            csvImporter.importToDB(session, "stores.csv", "\"epicentrRepo\".stores");
            csvImporter.importToDB(session, "types.csv", "\"epicentrRepo\".productTypes");

            SimpleStatement countTypesStatement =
                    SimpleStatement.newInstance("Select * from \"epicentrRepo\".productTypes");
            SimpleStatement countStoresStatement =
                    SimpleStatement.newInstance("Select * from \"epicentrRepo\".stores");

            int typesCount = session.execute(countTypesStatement).all().size();
            int storesCount = session.execute(countStoresStatement).all().size();

            Generate generate = new Generate(session, numberGenerateThreads);
            watch.start();
            generate.createProducts(typesCount, amountProducts);
            while (true) {
                if (generate.isGeneratingFinished()) {
                    watch.stop();
                    logger.info("Generating has finished successfully");
                    double generatingTime = watch.getTime() / 1000.0;
                    ResultSet resProd = session.execute("Select id from \"epicentrRepo\".products");
                    int countProds = resProd.all().size();
                    double productsPerSecond = countProds / generatingTime;
                    logger.info("GENERATING SPEED by {} threads: {} , total = {} products, elapseSeconds = {}"
                            , numberGenerateThreads, productsPerSecond, countProds, generatingTime);
                    break;
                }
            }
            watch.reset();
            Delivery delivery = new Delivery(session, numberDeliveryThreads);
            watch.start();
            delivery.deliverToStore(storesCount);
            while (true) {
                if (delivery.isDeliveryFinished()) {
                    watch.stop();
                    logger.info("Delivery has finished successfully");
                    double deliveryTime = watch.getTime() / 1000.0;
                    ResultSet resDelivery = session.execute("Select * from \"epicentrRepo\".delivery");
                    int countDelivery = resDelivery.all().size();
                    double deliveriesPerSecond = countDelivery / deliveryTime;
                    logger.info("DELIVERY SPEED by {} threads: {} , total = {} products, elapseSeconds = {}"
                            , numberDeliveryThreads, deliveriesPerSecond,
                            countDelivery, deliveryTime);
                    break;
                }
            }
            watch.reset();
            String cqlForNumberProductsInStoreByType = "SELECT * FROM \"epicentrRepo\"." +
                    "delivery WHERE type = ? and store = ? ";
            String cqlForInsertAvailability = "insert into \"epicentrRepo\"." +
                    "availability (type, store, quantity) values (?,?,?) if not exists";
            logger.debug("                                                    ");
            logger.debug("****************************************************");
            logger.debug("CQL ForNumberProductsInStoreByType: {}", cqlForNumberProductsInStoreByType);
            logger.debug("CQL ForInsertAvailability: {}", cqlForInsertAvailability);
            logger.debug("****************************************************");
            logger.debug("                                                    ");
            watch.start();
            Availability availability = new Availability();
            availability.insertAvailability(session, typesCount, storesCount,
                    cqlForNumberProductsInStoreByType, cqlForInsertAvailability);

            double filingAvailableTime = watch.getTime() / 1000.0;
            watch.reset();

            logger.info("                                                           ");
            logger.info("************************************************************");
            logger.info("AmountProduct assign in property file = {}", amountProducts);
            logger.info("FILLING AVAILABLE SPEED: {} ", filingAvailableTime);
            StoreFinder storeFinder = new StoreFinder(session, cqlExecutor);
            storeFinder.findStoreAddress(productType);

            logger.info("************************************************************");
            logger.info("                                                           ");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt(); // Reset interrupted status
            logger.error("Thread was interrupted {}", e.getMessage());
        }
    }
}