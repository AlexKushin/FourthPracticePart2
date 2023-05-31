package com.shpp.mentoring.okushin.task4p2;

import com.datastax.oss.driver.api.core.CqlSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.Validator;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

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
        this.productAmount = productAmount;
        this.numberThreads = numberThreads;
        this.typesCount = typesCount;
        this.validator = validator;


    }


    public void createProducts() {
        int amountForMainThreads = productAmount / (numberThreads - 1);
        int amountForAdditionalThread = productAmount - amountForMainThreads * (numberThreads - 1);
        logger.info("Validator instance created");
        ProductGenerator productGenerator = new ProductGenerator(validator);
        executorService = Executors.newFixedThreadPool(numberThreads);
        Future<?> future;
        for (int i = 0; i < numberThreads - 1; i++) {
            future =executorService.submit(new GenerateThread(session, productGenerator, amountForMainThreads, typesCount));
            try {
                future.get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt(); // Reset interrupted status
                logger.error("Thread was interrupted {}", e.getMessage());
            } catch (ExecutionException e) {
                //Throwable exception = e.getCause();
                logger.error("Error while execution task {}", e.getMessage());
                // Forward to exception reporter
            }
        }
        executorService.submit(new GenerateThread(session, productGenerator, amountForAdditionalThread, typesCount));

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
