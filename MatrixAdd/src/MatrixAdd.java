import java.io.IOException;
import java.util.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.GenericOptionsParser;

public class MatrixAdd{
	public static class MatrixAddMapper extends Mapper<Object, Text, Text, IntWritable>{
		private final static IntWritable i_value = new IntWritable(1);
		private Text word = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			StringTokenizer tokenizer = new StringTokenizer(value.toString());
			int row_id = Integer.parseInt(tokenizer.nextToken().trim());
			int col_id = Integer.parseInt(tokenizer.nextToken().trim());
			int m_value = Integer.parseInt(tokenizer.nextToken().trim());

			i_value.set(m_value);

			word.set(row_id + ", " + col_id);
			context.write(word, i_value);
			
		}
	}

	public static class MatrixAddReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
			int sum = 0;

			for(IntWritable val : values){
				sum += val.get();
			}

			result.set(sum);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		Job job = new Job(conf, "matrixadd");

		job.setJarByClass(MatrixAdd.class);
		job.setMapperClass(MatrixAddMapper.class);
		job.setReducerClass(MatrixAddReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}
}
