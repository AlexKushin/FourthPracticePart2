package com.shpp.mentoring.okushin.task4p2;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Delivery {
    private static final Logger logger = LoggerFactory.getLogger(Delivery.class);
   private CqlSession session;
    private int numberThreads;
    private  int storesCount;
     Random random;
    private CqlExecutor cqlExecutor;

    private boolean isDeliveryFinished=false;

    public Delivery(CqlSession session, int numberThreads, int storesCount,  CqlExecutor cqlExecutor) {
        random = new Random();
        this.session = session;
        this.numberThreads = numberThreads;
        this.storesCount = storesCount;
        this.cqlExecutor = cqlExecutor;
    }
    public Delivery(){}


    public void deliverToStore() throws InterruptedException {
        String cqlForSelectProducts = "SELECT * FROM \"epicentrRepo\".products";
        SimpleStatement searchStoreStatement = SimpleStatement.newInstance(cqlForSelectProducts);
        ResultSet res = session.execute(searchStoreStatement);
        List<Row> resRowList = res.all();
        ExecutorService service = Executors.newFixedThreadPool(numberThreads);

        for (int i = 0; i< numberThreads; i++){
            service.submit(new DeliveryThread(resRowList,session,storesCount,cqlExecutor));
            Thread.sleep(1000);
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
