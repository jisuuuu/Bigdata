import java.io.*;
import java.util.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.GenericOptionsParser;

public class PageRank{
	public static class PageRankMapper extends Mapper<LongWritable, Text, IntWritable, DoubleWritable>{
		private IntWritable one_key = new IntWritable();
		private DoubleWritable one_value = new DoubleWritable();
		private int n_pages;
		private double[] pagerank;

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			StringTokenizer itr = new StringTokenizer(value.toString());
			int n_links = itr.countTokens() - 1;
			if(n_links == 0)
				return;

			int src_id = Integer.parseInt(itr.nextToken().trim());
			double pr = pagerank[src_id] / (double)n_links;
			one_value.set(pr);

			//연결된 page에 page rank 값 분배하는 코드 필요
			while(itr.hasMoreTokens()){
				one_key.set(Integer.parseInt(itr.nextToken().trim()));
				context.write(one_key, one_value);
			}
		}

		protected void setup(Context context) throws IOException, InterruptedException{
			Configuration conf = context.getConfiguration();
			n_pages = conf.getInt("n_pages", -1);
			pagerank = new double[n_pages];

			for(int i = 0; i < n_pages; i++){
				pagerank[i] = conf.getFloat("pagerank" + i, 0);
			}
		}
	}

	public static class PageRankReducer extends Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable>{
		private DoubleWritable result = new DoubleWritable();
		private double damping_factor = 0.85;
		private int n_pages;

		public void reduce(IntWritable key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException{
			double agg_val = 0;

			agg_val = (1.0 - damping_factor) / (double)n_pages;
			//새로운 page rank 값 구하는 코드 필요
			for(DoubleWritable val : values){
				agg_val = agg_val + damping_factor * val.get();
			}

			result.set(agg_val);
			context.write(key, result);
		}

		protected void setup(Context context) throws IOException, InterruptedException{
			Configuration conf = context.getConfiguration();
			n_pages = conf.getInt("n_pages", -1);
		}
	}

	public static void main(String[] args) throws Exception{
		int n_pages = 4;
		int n_iter = 3;
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		if(otherArgs.length != 2){
			System.err.println("Usage: PageRank<in>");
			System.exit(2);
		}

		initPageRank(conf, n_pages);
		for(int i = 0; i < n_iter; i++){
			Job job = new Job(conf, "pagerank");

			job.setJarByClass(PageRank.class);
			job.setMapperClass(PageRankMapper.class);
			job.setReducerClass(PageRankReducer.class);
			job.setOutputKeyClass(IntWritable.class);
			job.setOutputValueClass(DoubleWritable.class);
			job.setInputFormatClass(TextInputFormat.class);
			FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
			FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
			FileSystem.get(job.getConfiguration()).delete(new Path(otherArgs[1]), true);
			job.waitForCompletion(true);
			updatePageRank(conf, n_pages);
		}
	}

	public static void initPageRank(Configuration conf, int n_pages){
		conf.setInt("n_pages", n_pages);
		for(int i = 0; i < n_pages; i++)
			conf.setFloat("pagerank" + i, (float)(1.0 / (double)n_pages));
	}

	public static void updatePageRank(Configuration conf, int n_pages) throws Exception{
		FileSystem dfs = FileSystem.get(conf);
		Path filenamePath = new Path("/user/bigdata/output/part-r-00000");
		FSDataInputStream in = dfs.open(filenamePath);
		BufferedReader reader = new BufferedReader(new InputStreamReader(in));

		String line = reader.readLine();
		while (line != null){
			StringTokenizer itr = new StringTokenizer(new String(line));
			int src_id = Integer.parseInt(itr.nextToken().trim());
			double pr = Double.parseDouble(itr.nextToken().trim());

			conf.setFloat("pagerank" + src_id, (float)pr);
			line = reader.readLine();
		}
	}

}


