package com.neuedu.myhdfs.myjava;

import java.io.Serializable;

public class Student implements Serializable {

    private static final long serioVersionUID = 42123413452453L;
    private String id;
    private String name;
    private Integer age;
    public Student() {
    }

    public Student(String id, String name, Integer age) {
        this.id = id;
        this.name = name;
        this.age = age;
    }

    @Override
    public String toString() {
        return "Student{" +
                "id='" + id + '\'' +
                ", name='" + name + '\'' +
                ", age=" + age +
                '}';
    }

}
