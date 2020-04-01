import java.io.*;
import java.util.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.GenericOptionsParser;
class DoubleString implements WritableComparable{
	String joinKey = new String();
	Integer idx;

	public DoubleString(){}
	public DoubleString(String _joinKey, Integer _idx){
		this.joinKey = _joinKey;
		this.idx = _idx;
	}

	public void readFields(DataInput in) throws IOException{
		joinKey = in.readUTF();
		idx = in.readInt();
	}

	public void write(DataOutput out) throws IOException{
		out.writeUTF(joinKey);
		out.writeInt(idx);
	}

	public int compareTo(Object o1){
		DoubleString o = (DoubleString) o1;

		int ret = joinKey.compareTo(o.joinKey);
		if(ret != 0)
			return ret;

		return idx.compareTo(o.idx);
	}

	public String toString(){
		return joinKey + " " + idx;
	}

}
public class MatrixMul2{
	public static class CompositeKeyComparator extends WritableComparator{
		protected CompositeKeyComparator(){
			super(DoubleString.class, true);
		}

		public int compare(WritableComparable w1, WritableComparable w2){
			DoubleString k1 = (DoubleString)w1;
			DoubleString k2 = (DoubleString)w2;

			int result = k1.joinKey.compareTo(k2.joinKey);

			if(result == 0)
				result =  k1.idx.compareTo(k2.idx);
			return result;
		}
	}

	public static class FirstPartitioner extends Partitioner<DoubleString, Text>{
		public int getPartition(DoubleString key, Text value, int numPartition){
			return key.joinKey.hashCode() % numPartition;
		}
	}
	
	public static class FirstGroupingComparator extends WritableComparator{
		protected FirstGroupingComparator(){
			super(DoubleString.class, true);
		}

		public int compare(WritableComparable w1, WritableComparable w2){
			DoubleString k1 = (DoubleString)w1;
			DoubleString k2 = (DoubleString)w2;

			return k1.joinKey.compareTo(k2.joinKey);
		}
	}
	public static class MatrixMul2Mapper extends Mapper<Object, Text, DoubleString, IntWritable>{
		private DoubleString key_value;
		private IntWritable i_value = new IntWritable();

		private int m_value;
		private int k_value;
		private int n_value;

		private boolean isA = false;
		private boolean isB = false;

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			StringTokenizer tokenizer = new StringTokenizer(value.toString());
			int row_id = Integer.parseInt(tokenizer.nextToken().trim());
			int col_id = Integer.parseInt(tokenizer.nextToken().trim());
			int matrix_value = Integer.parseInt(tokenizer.nextToken().trim());

			i_value.set(matrix_value);
			
			if(isA){
				for(int i = 0; i < n_value; i++){
					key_value = new DoubleString(row_id + " " + i, col_id);
					context.write(key_value, i_value);
				}
			}
			else if(isB){
				for(int i = 0; i < m_value; i++){
					key_value = new DoubleString(i + " " + col_id, row_id);
					context.write(key_value, i_value);
				}
			}
		}

		public void setup(Context context) throws IOException, InterruptedException{
			Configuration conf = context.getConfiguration();

			m_value = conf.getInt("m", -1);
			k_value = conf.getInt("k", -1);
			n_value = conf.getInt("n", -1);

			String filename = ((FileSplit)context.getInputSplit()).getPath().getName();

			if(filename.equals("matrix_a"))
				isA = true;
			if(filename.equals("matrix_b"))
				isB = true;
		}
	}

	public static class MatrixMul2Reducer extends Reducer<DoubleString, IntWritable, Text, IntWritable>{
		private IntWritable result = new IntWritable();
		private Text word = new Text();

		public void reduce(DoubleString key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
			int mul = 1;
			int cnt = 0;
			int total = 0;

			//내가 구현 해야행
			for(IntWritable val : values){
				mul *= val.get();
				cnt++;
				
				if(cnt % 2 == 0){
					total += mul;
					mul = 1;
				}	
			}

			word.set(key.joinKey);
			result.set(total);
			context.write(word, result);
		}
	}

	public static void main(String[] args) throws Exception{
		int m_value = 2;
		int k_value = 2;
		int n_value = 2;

		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		if(otherArgs.length != 2){
			System.err.println("Usage : MatrixMul <in> <out>");
			System.exit(2);
		}

		conf.setInt("m", m_value);
		conf.setInt("k", k_value);
		conf.setInt("n", n_value);

		Job job = new Job(conf, "matrix mul2");

		job.setJarByClass(MatrixMul2.class);
		job.setMapperClass(MatrixMul2Mapper.class);
		job.setReducerClass(MatrixMul2Reducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setMapOutputKeyClass(DoubleString.class);
		job.setMapOutputKeyClass(IntWritable.class);

		job.setPartitionerClass(FirstPartitioner.class);
		job.setGroupingComparatorClass(FirstGroupingComparator.class);
		job.setSortComparatorClass(CompositeKeyComparator.class);

		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		FileSystem.get(job.getConfiguration()).delete(new Path(otherArgs[1]), true);

		job.waitForCompletion(true);

	}
}
