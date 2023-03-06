package com.neuedu.myhdfs.myjava;
import org.apache.hadoop.io.WritableComparable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

public class StudentOfHadoop implements WritableComparable<StudentOfHadoop>{

    private String id;
    private String name;
    private Integer age;
    public StudentOfHadoop(){

    }

    public StudentOfHadoop(String id, String name, Integer age) {
        this.id = id;
        this.name = name;
        this.age = age;
    }

    @Override
    public String toString() {
        return "StudentOfHadoop{" +
                "id='" + id + '\'' +
                ", name='" + name + '\'' +
                ", age=" + age +
                '}';
    }

    public int compareTo(StudentOfHadoop o) {
        if (null == o){
            return 1;
        }
        return this.id.compareTo(o.id);
    }

    public void write(DataOutput dataOutput) throws IOException {
        //hadoop序列化
        dataOutput.writeUTF(this.id);
        dataOutput.writeUTF(this.name);
        dataOutput.writeInt(this.age);

    }

    public void readFields(DataInput dataInput) throws IOException {

        this.id = dataInput.readUTF();
        this.name = dataInput.readUTF();
        this.age = dataInput.readInt();

    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
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
