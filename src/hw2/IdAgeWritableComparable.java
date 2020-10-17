package hw2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
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
	
    @Override
    public void write(DataOutput out) throws IOException {
    	id.write(out);
    	age.write(out);
    }
	
    @Override
    public void readFields(DataInput in) throws IOException {
    	id.readFields(in);
    	age.readFields(in);
    }
	
    @Override
	public int hashCode() {
		return id.hashCode();
	}

	@Override
	public int compareTo(Object o) {
		IdAgeWritableComparable o2 = (IdAgeWritableComparable) o;	// hacky but whatever
		int idCmp = Integer.parseInt(id.toString()) - Integer.parseInt(o2.id.toString());
		return (idCmp == 0) ? age.get()-o2.age.get() : idCmp;
	}
}