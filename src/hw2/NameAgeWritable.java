package hw2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class NameAgeWritable implements Writable {
    public Text fname;
    public IntWritable age;
    
    public NameAgeWritable() {
    	fname = new Text("");
    	age = new IntWritable(0);
    }
    
    public NameAgeWritable(String fn, int a) {
    	fname = new Text(fn);
    	age = new IntWritable(a);
    }
    
    public void write(DataOutput out) throws IOException {
    	fname.write(out);
    	age.write(out);
    }
    
    public void readFields(DataInput in) throws IOException {
    	fname.readFields(in);
    	age.readFields(in);
    }
}