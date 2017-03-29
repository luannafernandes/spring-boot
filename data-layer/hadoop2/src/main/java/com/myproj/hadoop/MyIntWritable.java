package com.myproj.hadoop;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by dumin on 3/28/17.
 */

/**
 * A WritableComparable for ints.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class MyIntWritable implements WritableComparable<MyIntWritable> {
    private int value;

    public MyIntWritable() {
    }

    public MyIntWritable(int value) {
        set(value);
    }

    /**
     * Set the value of this IntWritable.
     */
    public void set(int value) {
        this.value = value;
    }

    /**
     * Return the value of this IntWritable.
     */
    public int get() {
        return value;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        value = in.readInt();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(value);
    }

    /**
     * Returns true iff <code>o</code> is a IntWritable with the same value.
     */
    @Override
    public boolean equals(Object o) {
        if (!(o instanceof MyIntWritable))
            return false;
        MyIntWritable other = (MyIntWritable) o;
        return this.value == other.value;
    }

    @Override
    public int hashCode() {
        return value;
    }

    /**
     * Compares two IntWritables.
     */
    @Override
    public int compareTo(MyIntWritable o) {
        int thisValue = this.value;
        int thatValue = o.value;
        return (thisValue > thatValue ? -1 : (thisValue == thatValue ? 0 : 1));
    }

    @Override
    public String toString() {
        return Integer.toString(value);
    }

    /**
     * A IntComparator optimized for IntWritable.
     */
    public static class Comparator extends WritableComparator {
        public Comparator() {
            super(IntWritable.class);
        }

        @Override
        public int compare(byte[] b1, int s1, int l1,
                           byte[] b2, int s2, int l2) {
            int thisValue = readInt(b1, s1);
            int thatValue = readInt(b2, s2);
            return (thisValue > thatValue ? -1 : (thisValue == thatValue ? 0 : 1));
        }
    }

    static {                                        // register this comparator
        WritableComparator.define(MyIntWritable.class, new Comparator());
    }
}


