package com.shpp.mentoring.okushin.task4p2;

import com.datastax.oss.driver.api.core.CqlSession;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GenerateThread implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(GenerateThread.class);
    private ProductGenerator productGenerator;
    int amount;
    private int typesCount;

    private CqlSession session;


    public GenerateThread(CqlSession session, ProductGenerator productGenerator, int amount, int typesCount) {
        this.productGenerator = productGenerator;
        this.amount = amount;
        this.typesCount = typesCount;
        this.session = session;
    }

    public GenerateThread() {
    }


    @Override
    public void run() {
        logger.info("GenerateThread starts");
        productGenerator.insertValidatedProducts(session, amount, typesCount);

    }
}