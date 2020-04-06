import java.io.*;
import java.util.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.GenericOptionsParser;

public class ReduceSideJoin2{
	public class DoubleString implements WritableComparable{
		String joinKey = new String();
		String tableName = new String();

		public DoubleString(){}
		public DoubleString(String _joinKey, String _tableName){
			this.joinKey = _joinKey;
			this.tableName = _tableName;
		}

		public void readFields(DataInput in) throws IOException{
			joinKey = in.readUTF():
			tableName = in.readUTF();
		}
		
		public void write(DataOutput out) throws IOException{
			out.writeUTF(joinKey);
			out.writeUTF(tableName);
		}

		public int compareTo(Object o1){
			DoubleString o = (DoubleString)o1;
			int ret = joinKey.compareTo(o.joinKey);
			
			if(ret != 0)
				return ret;
			return -1 * tableName.compareTo(o.tableName);
		}

		public String toString(){
			return joinKey + " " + tableName;
		}

	}

	public static class CompositeKeyComparator extends WritableComparator{
		protected CompositeKeyComparator(){
			super(DoubleString.class, true);
		}

		public int compare(WritableComparable w1, WritableComparable w2){
			DoubleString k1 = (DoubleString)w1;
			DoubleString k2 = (DoubleString)w2;

			int result = k1.joinKey.compareTo(k2.joinKey);

			if(0 == result){
				result = -1 * k1.tableName.compareTo(k2.tableName);
			}

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

	public static class ReduceSideJoinMapper extends Mapper<Object, Text, DoubleString, Text>{
		boolean fileA = true;
		protected void setup(Context context) throws IOException, InterruptedException{
			String filename = ((FileSplit)context.getInputSplit()).getPath().getName();
			
			if(filename.indexOf("relation_a") != -1)
				fileA = true;
			else
				fileA = false;
		}

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			StringTokenizer tokenizer = new StringTokenizer(value.toString(), "|");
			Text outputKey = new Text();
			Text outputValue = new Text();
			
			String joinKey = "";
			String o_value = "";

			if(fileA){
			//its mine
				o_value += "A ";
				o_value += tokenizer.nextToken().trim() + " ";
				o_value += tokenizer.nextToken().trim() + " ";

				joinKey += tokenizer.nextToken().trim();
			}
			else{
				joinKey += tokenizer.nextToken().trim();

				o_value += "B ";
				o_value += tokenizer.nextToken().trim();
			}
			
			outputKey.set(joinKey);
			outputValue.set(o_value);
			context.write(outputKey, outputValue);
		}
	}

	public static class ReduceSideJoinReducer extends Reducer<DoubleString, Text, Text, Text>{
		public void reduce(DoubleString key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
		Text reduce_key = new Text();
		Text reduce_result = new Text();
		String description = "";
		ArrayList<String> buffer = new ArrayList<String>();

		for(Text val : values){
			StringTokenizer tokenizer = new StringTokenizer(val.toString());
			String file_type = tokenizer.nextToken().trim();
			
			if(file_type.equals("B")){
				description = tokenizer.nextToken().trim();
			}
			else{
				if(description.equals("")){
					buffer.add(val.toString());	
				}
				else{
					reduce_key.set(tokenizer.nextToken().trim());
					reduce_result.set(tokenizer.nextToken().trim() + " " + description);

					context.write(reduce_key, reduce_result);
				}
			}
		}

		for(int i = 0; i< buffer.size(); i++){
			StringTokenizer tokenizer = new StringTokenizer(buffer.get(i));
			String a = tokenizer.nextToken().trim();

			reduce_key.set(tokenizer.nextToken().trim());
			reduce_result.set(tokenizer.nextToken().trim() + " " + description);

			context.write(reduce_key, reduce_result);
		}
	}

	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if(otherArgs.length != 2){
			System.err.println("Usage : TopK <in> <out>");
			System.exit(2);
		}

		Job job = new Job(conf, "ReduceSideJoin");
		job.setJarByClass(ReduceSideJoin.class);
		job.setMapperClass(ReduceSideJoinMapper.class);
		job.setReducerClass(ReduceSideJoinReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setPartitionerClass(FirstPartitioner.class);
		job.setGroupingComparatorClass(FirstGroupingComparator.class);
		job.setSortComparatorClass(CompositeKeyComparator.class);
        }


		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		FileSystem.get(job.getConfiguration()).delete(new Path(otherArgs[1]), true);
		
		System.exit(job.waitForCompletion(true)? 0 : 1);
	}

}
