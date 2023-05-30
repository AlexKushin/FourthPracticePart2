package com.shpp.mentoring.okushin.task4p2;

import com.datastax.oss.driver.api.core.CqlSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.Validator;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Generate {
    private static final Logger logger = LoggerFactory.getLogger(Delivery.class);
    CqlSession session;
    int numberThreads;
    private final int typesCount;
    Random random;
    ExecutorService executorService;
    Validator validator;
    int productAmount;
    boolean isGeneratingFinished = false;

    public Generate(CqlSession session, int numberThreads, int typesCount, Validator validator, int productAmount) {
        random = new Random();
        this.session = session;
        this.numberThreads = numberThreads;
        this.typesCount = typesCount;
        this.validator = validator;
        this.productAmount = productAmount;
        executorService = Executors.newFixedThreadPool(numberThreads);
    }


    public void createProducts() {
        CqlExecutor cqlExecutor = new CqlExecutor();
        int amountForMainThreads = productAmount / (numberThreads - 1);
        int amountForAdditionalThread = productAmount - amountForMainThreads * (numberThreads - 1);
        logger.info("Validator instance created");
        ProductGenerator productGenerator = new ProductGenerator(validator);

        for (int i = 0; i < numberThreads - 1; i++) {
            executorService.submit(new GenerateThread(session, productGenerator, cqlExecutor, amountForMainThreads, typesCount));
        }
        executorService.submit(new GenerateThread(session, productGenerator,cqlExecutor, amountForAdditionalThread, typesCount));
        executorService.shutdown();
        while (true) {
            if (executorService.isTerminated()) {
                isGeneratingFinished = true;
                logger.info("Creating has finished successfully");
                return;
            }
        }
    }

    public boolean isGeneratingFinished() {
        return this.isGeneratingFinished;
    }
}
