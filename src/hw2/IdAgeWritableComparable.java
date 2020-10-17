package hw2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class IdAgeWritableComparable implements WritableComparable {
    public Text id;
    public IntWritable age;
    
    public IdAgeWritableComparable() {
    	id = new Text("");
    	age = new IntWritable(0);
    }
    
    public IdAgeWritableComparable(String i, int a) {
    	id = new Text(i);
    	age = new IntWritable(a);
    }
    
    public void write(DataOutput out) throws IOException {
    	id.write(out);
    	age.write(out);
    }
    
    public void readFields(DataInput in) throws IOException {
    	id.readFields(in);
    	age.readFields(in);
    }

	public int compareTo(IdAgeWritableComparable o) {
		int strCmp = id.toString().compareTo(o.id.toString());
		int ageA = age.get();
		int ageB = o.age.get();
		
		if (strCmp == 0) {
			if 		(ageA == ageB) 	return 0;
			else if (ageA < ageB) 	return -1;
			else					return 1;
		} else {
			return strCmp;
		}
		
	}
	
	public int hashCode() {
		return 0;
	}

	@Override
	public int compareTo(Object o) {
		// TODO Auto-generated method stub
		return 0;
	}
}