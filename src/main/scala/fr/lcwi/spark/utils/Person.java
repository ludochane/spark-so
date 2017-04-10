package fr.lcwi.spark.utils;

import java.io.Serializable;

/**
 * Created by chanelu on 07/04/2017.
 */
public class Person implements Serializable {
    private static final long serialVersionUID = 1L;

    private String name;
    private Integer age;

    public Person(String name, Integer age) {
        this.name = name;
        this.age = age;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getAge() {
        return age;
    }

    public void setAge(Integer age) {
        this.age = age;
    }
}
