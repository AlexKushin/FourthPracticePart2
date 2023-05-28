package com.shpp.mentoring.okushin.task4p2;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;

import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Delivery {
    CqlSession session;
    int numberThreads;
    private final int storesCount;
    Random random;

    public Delivery(CqlSession session, int numberThreads, int storesCount) {
        random = new Random();
        this.session = session;
        this.numberThreads = numberThreads;
        this.storesCount = storesCount;
    }


    public void deliverToStore() throws InterruptedException {
        String cqlForSelectProducts = "SELECT * FROM \"epicentrRepo\".products";
        SimpleStatement searchStoreStatement = SimpleStatement.newInstance(cqlForSelectProducts);
        ResultSet res = session.execute(searchStoreStatement);
        List<Row> resRowList = res.all();
        ExecutorService service = Executors.newFixedThreadPool(numberThreads);

        for (int i = 0; i< numberThreads; i++){
            service.submit(new DeliveryThread(resRowList,session,storesCount));
            Thread.sleep(500);
        }
        service.shutdown();
    }


}
