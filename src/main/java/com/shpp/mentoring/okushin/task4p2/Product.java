package com.shpp.mentoring.okushin.task4p2;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;

public class Product {
    //@NotNull(message = "Name cannot be null")
    //@Size(min = 7, message = "Name must be more than 7 characters") //medium
    //@Pattern(regexp = ".*a.*") //hard
    String name;
    @Min(1)
    @Max(120)
    int typeId;

    public Product(String name, int typeId) {
        this.name = name;
        this.typeId = typeId;


    }


    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getTypeId() {
        return typeId;
    }

    public void setTypeId(int typeId) {
        this.typeId = typeId;
    }


}
