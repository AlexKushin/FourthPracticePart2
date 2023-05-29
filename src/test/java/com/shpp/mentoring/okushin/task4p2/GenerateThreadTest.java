package com.shpp.mentoring.okushin.task4p2;

import com.datastax.oss.driver.api.core.CqlSession;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class GenerateThreadTest extends GenerateThread {

    public GenerateThreadTest(CqlSession session, ProductGenerator productGenerator, int amount, int typesCount) {
        super(session, productGenerator, amount, typesCount);
    }

    @Test
    void testRun() {
    }
}