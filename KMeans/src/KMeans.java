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

public class KMeans{
	public static class KMeansMapper extends Mapper<LongWritable, Text, IntWritable, Text>{
		private IntWritable one_key = new IntWritable();

		private int n_centers;
		private double[] center_x;
		private double[] center_y;

		protected void setup(Context context) throws IOException, InterruptedException{
			Configuration conf = context.getConfiguration();
			n_centers = conf.getInt("n_centers", -1);
			center_x = new double[n_centers];
			center_y = new double[n_centers];

			for(int i = 0; i < n_centers; i++){
				center_x[i] = conf.getDouble("center_x_" + i, 0);
				center_y[i] = conf.getDouble("center_y_" + i, 0);
			}
		}

		public double getDist(double x1, double y1, double x2, double y2){
			double dist = (x1 - x2) * (x1 -x2) + (y1 - y2) * (y1 - y2);
			return Math.sqrt(dist);
		}

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			StringTokenizer tokenizer = new StringTokenizer(value.toString());

			if(tokenizer.countTokens() < 2) return;
			if(n_centers == 0) return;

			double x = Double.parseDouble(tokenizer.nextToken().trim());
			double y = Double.parseDouble(tokenizer.nextToken().trim());
			int cluster_idx = 0;

			//its mine
			double result = getDist(x, y, center_x[0], center_y[0]);
			for(int i = 1; i < n_centers; i++){
				if(getDist(x, y, center_x[i], center_y[i]) < result){
					cluster_idx = i;
					result = getDist(x, y, center_x[i], center_y[i]);
				}
			}

			one_key.set(cluster_idx);
			context.write(one_key, value);
			
		}
	}

	public static class KMeansReducer extends Reducer<IntWritable, Text, IntWritable, Text>{
		public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			double x_total = 0;
			double y_total = 0;
			int cnt = 0;
			Text result = new Text();

			for(Text val : values){
				//new center calcurate
				StringTokenizer tokenizer = new StringTokenizer(val.toString());
				cnt++;

				x_total = Double.parseDouble(tokenizer.nextToken().trim());
				y_total = Double.parseDouble(tokenizer.nextToken().trim());
			}

			result.set(x_total / cnt + " " + y_total / cnt);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if(otherArgs.length != 3){
			System.err.println("Usage : KMeans <in>");
			System.exit(2);
		}
		int n_centers = Integer.parseInt(otherArgs[2]);

		initCenterPoint(conf, n_centers);
		
		double[] before_center_x = new double[n_centers];
		double[] before_center_y = new double[n_centers];

		double[] after_center_x = new double[n_centers];
		double[] after_center_y = new double[n_centers];
		while(true){
			for(int i = 0; i < n_centers; i++){
				before_center_x[i] = conf.getDouble("center_x_" + i, 0);
				before_center_y[i] = conf.getDouble("center_y_" + i, 0);
			}

			Job job = new Job(conf, "KMeans");
			job.setJarByClass(KMeans.class);
			job.setMapperClass(KMeansMapper.class);
			job.setReducerClass(KMeansReducer.class);
			job.setOutputKeyClass(IntWritable.class);
			job.setOutputValueClass(Text.class);
			job.setInputFormatClass(TextInputFormat.class);
			FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
			FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
			FileSystem.get(job.getConfiguration()).delete(new Path(otherArgs[1]), true);

			updateCenterPoint(conf, n_centers);

			for(int i = 0; i < n_centers; i++){
				after_center_x[i] = conf.getDouble("center_x_" + i, 0);
				after_center_y[i] = conf.getDouble("center_y_" + i, 0);
			}

			if(Arrays.equals(before_center_x, after_center_x) && Arrays.equals(before_center_y, after_center_y))
				break;

		}
	}

	public static void initCenterPoint(Configuration conf, int n_centers){
		conf.setInt("n_centers", n_centers);
		Random rm = new Random();

		for(int i = 0 ; i < n_centers; i++){
			conf.setDouble("center_x_" + i, rm.nextFloat() * 10);
			conf.setDouble("center_y_" + i, rm.nextFloat() * 10);
		}
	}

	public static void updateCenterPoint(Configuration conf, int n_centers) throws Exception{
		FileSystem dfs = FileSystem.get(conf);
		Path filenamePath = new Path("/output/part-r-00000");
		FSDataInputStream in = dfs.open(filenamePath);
		BufferedReader reader = new BufferedReader(new InputStreamReader(in));

		String line = reader.readLine();
		while(line != null){
			StringTokenizer itr = new StringTokenizer(new String(line));
			int src_id = Integer.parseInt(itr.nextToken().trim());
			double center_x = Double.parseDouble(itr.nextToken().trim());
			double center_y = Double.parseDouble(itr.nextToken().trim());
			conf.setDouble("center_x_" + src_id, center_x);
			conf.setDouble("center_y_" + src_id, center_y);
			line = reader.readLine();
		}
	}
}
