package com.shpp.mentoring.okushin.task4p2;

import com.datastax.oss.driver.api.core.CqlSession;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GenerateThread implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(GenerateThread.class);
    ProductGenerator productGenerator;
    int amount;
    private final int typesCount;

    private final CqlSession session;


    public GenerateThread(CqlSession session, ProductGenerator productGenerator, int amount, int typesCount) {
        this.productGenerator = productGenerator;
        this.amount = amount;
        this.typesCount = typesCount;

        this.session = session;
    }


    @Override
    public void run() {
        logger.info("GenerateThread starts");
        productGenerator.insertValidatedProducts(session, amount, typesCount);

    }
}