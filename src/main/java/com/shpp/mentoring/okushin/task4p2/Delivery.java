package com.shpp.mentoring.okushin.task4p2;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class Delivery {
    private static final Logger logger = LoggerFactory.getLogger(Delivery.class);
   private CqlSession session;
    private int numberThreads;
     Random random;
    private boolean isDeliveryFinished=false;

    public Delivery(CqlSession session, int numberThreads ) {
        random = new Random();
        this.session = session;
        this.numberThreads = numberThreads;
    }
    public Delivery(){}


    public void deliverToStore(int storesCount) {
        String cqlForSelectProducts = "SELECT * FROM \"epicentrRepo\".products";
        SimpleStatement searchStoreStatement = SimpleStatement.newInstance(cqlForSelectProducts);
        ResultSet res = session.execute(searchStoreStatement);
        List<Row> resRowList = res.all();
        ExecutorService service = Executors.newFixedThreadPool(numberThreads);

        for (int i = 0; i< numberThreads; i++){
            Future<?> future =service.submit(new DeliveryThread(resRowList,session,storesCount));
            while (future.isDone()) {
                try {
                    future.get();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt(); // Reset interrupted status - for a what???
                    logger.error("Thread was interrupted {}", e.getMessage());
                } catch (ExecutionException e) {
                    logger.error("Error while execution task {}", e.getMessage());
                    // Forward to exception reporter
                }
            }
        }
        service.shutdown();
        while (true) {
            if (service.isTerminated()) {
                isDeliveryFinished = true;
                logger.info("Delivery has finished successfully");
                return;
            }
        }
    }
    public boolean isDeliveryFinished(){
        return  this.isDeliveryFinished;
    }

}
