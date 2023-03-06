package com.neuedu.myhdfs.myjava;

import java.io.*;

public class Mytest {
    public static void main(String[] args) throws IOException,ClassNotFoundException {
        Student s1 = new Student("S10001","张三",22);
        System.out.println(s1);

        //序列化
        FileOutputStream fout = new FileOutputStream("d:\\testdir\\s\\d.dat");
        ObjectOutputStream objectOutputStream = new ObjectOutputStream(fout);
        objectOutputStream.writeObject(s1);
        objectOutputStream.close();
        fout.close();
        System.out.println("序列化完成");

        //反序列化
        FileInputStream fin = new FileInputStream("d:\\testdir\\s\\d.dat");
        ObjectInputStream objectInputStream = new ObjectInputStream(fin);
        Student s2 = (Student) objectInputStream.readObject();
        fin.close();
        System.out.println("反序列化完成，信息为"+s2);


    }
}
