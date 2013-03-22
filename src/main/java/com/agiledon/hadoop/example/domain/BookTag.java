package com.agiledon.hadoop.example.domain;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: Zhang Yi
 * Date: 3/16/13
 * Time: 8:53 PM
 */
public class BookTag implements Writable {
    private String name;
    private int count;

    public BookTag() {
        count = 0;
    }

    public BookTag(String name, int count) {
        this.name = name;
        this.count = count;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        if (dataOutput != null) {
            Text.writeString(dataOutput, name);
            dataOutput.writeInt(count);
        }
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        if (dataInput != null) {
            name = Text.readString(dataInput);
            count = dataInput.readInt();
        }
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    @Override
    public String toString() {
        return "BookTag{" +
                "name='" + name + '\'' +
                ", count=" + count +
                '}';
    }
}
