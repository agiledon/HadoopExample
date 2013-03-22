package com.agiledon.hadoop.example.domain;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: Zhang Yi
 * Date: 3/16/13
 * Time: 8:48 PM
 */
public class BookTags implements Writable {
    private Map<String, BookTag> tags = new HashMap<String, BookTag>();

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(tags.size());
        for (BookTag tag : tags.values()) {
            tag.write(dataOutput);
        }
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        int size = dataInput.readInt();
        for (int i = 0; i < size; i++) {
            BookTag tag = new BookTag();
            tag.readFields(dataInput);
            tags.put(tag.getName(), tag);
        }
    }

    public void add(BookTag tag) {
            String tagName = tag.getName();
            if (tags.containsKey(tagName)) {
                BookTag bookTag = tags.get(tagName);
                bookTag.setCount(bookTag.getCount() + tag.getCount());
            } else {
                tags.put(tagName, tag);
            }
    }

    @Override
    public String toString() {
        StringBuilder resultTags = new StringBuilder();
        for (BookTag tag : tags.values()) {
            resultTags.append(tag.toString());
            resultTags.append("|");
        }
        return resultTags.toString();
    }
}
