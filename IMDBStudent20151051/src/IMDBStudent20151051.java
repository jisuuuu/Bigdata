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
class IMDB{
	public String title;
	public double avg_rating;
	
	public IMDB(String _title, double _avg_rating){
		this.title = _title;
		this.avg_rating = _avg_rating;
	}

	public String getString(){
		return title + "|" + avg_rating;
	}
}
public class IMDBStudent20151051{

	public static class IMDBComparator implements Comparator<IMDB>{
		public int compare(IMDB x, IMDB y){
			if(x.avg_rating > y.avg_rating) return 1;
			if(x.avg_rating < y.avg_rating) return -1;
			return 0;	
		}
	}

	public static void insertIMDB(PriorityQueue q, String title, double avg_rating, int topK){
		IMDB imdb_head = (IMDB)q.peek();

		if(q.size() < topK || imdb_head.avg_rating <= avg_rating){
			IMDB imdb = new IMDB(title, avg_rating);
			q.add(imdb);

			if(q.size() > topK)
				q.remove();
		}
	}

	public static class IMDBMapper1 extends Mapper<Object, Text, Text, Text>{
		boolean fileM = true;
		protected void setup(Context context) throws IOException, InterruptedException{
			String filename = ((FileSplit)context.getInputSplit()).getPath().getName();
			
			if(filename.indexOf("movies.dat") != -1)
				fileM = true;
			else
				fileM = false;
		}

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			StringTokenizer tokenizer = new StringTokenizer(value.toString(), "::");
			Text outputKey = new Text();
			Text outputValue = new Text();
			
			String joinKey = "";
			String o_value = "";
			boolean fantasy = false;

			if(fileM){
				joinKey += tokenizer.nextToken().trim();

				o_value += "M|";
				o_value += tokenizer.nextToken().trim();

				String genre = tokenizer.nextToken().trim();

				StringTokenizer itr = new StringTokenizer(genre, "|");
				
				while(itr.hasMoreTokens()){
					String data = itr.nextToken().trim();

					if(data.equals("Fantasy"))
							fantasy = true;
				}

				if(fantasy == true){
					outputKey.set(joinKey);
					outputValue.set(o_value);
					context.write(outputKey, outputValue);
				}
			}
			else{
				String userId = tokenizer.nextToken().trim();
				joinKey += tokenizer.nextToken().trim();

				o_value += "R|";
				o_value += tokenizer.nextToken().trim();
				
				outputKey.set(joinKey);
				outputValue.set(o_value);
				context.write(outputKey, outputValue);
			}
		}
	}

	public static class IMDBReducer1 extends Reducer<Text, Text, Text, DoubleWritable>{
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			Text reduce_key = new Text();
			DoubleWritable average = new DoubleWritable();
			String description = "";
			ArrayList<String> buffer = new ArrayList<String>();
			double average_sum = 0;
			int cnt = 0;

			for(Text val : values){
				StringTokenizer tokenizer = new StringTokenizer(val.toString(), "|");
				String file_type = tokenizer.nextToken().trim();
			
				if(file_type.equals("M")){
					description = tokenizer.nextToken().trim();
					reduce_key.set(description + "|");
				}
				else{
					if(description.equals("")){
						buffer.add(val.toString());	
					}
					else{
						cnt++;
						average_sum += Double.parseDouble(tokenizer.nextToken().trim());
					}
				}
			}

			for(int i = 0; i< buffer.size(); i++){
				StringTokenizer tokenizer = new StringTokenizer(buffer.get(i), "|");
				String a = tokenizer.nextToken().trim();
				
				cnt++;
				average_sum += Double.parseDouble(tokenizer.nextToken().trim());
			}
			
			if(!description.equals("")){
				average.set(average_sum / cnt);
				context.write(reduce_key, average);
			}
		}
	}

	public static class IMDBMapper2 extends Mapper<Object, Text, Text, NullWritable>{
		private PriorityQueue<IMDB> queue;
		private Comparator<IMDB> comp = new IMDBComparator();
		private int topK;

		protected void setup(Context context) throws IOException, InterruptedException{
			Configuration conf = context.getConfiguration();
			topK = conf.getInt("topK", -1);
			queue = new PriorityQueue<IMDB>(topK, comp);
		}

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			StringTokenizer tokenizer = new StringTokenizer(value.toString(), "|");

			String title = tokenizer.nextToken().trim();
			double avg_rating = Double.parseDouble(tokenizer.nextToken().trim());

			insertIMDB(queue, title, avg_rating, topK);
		}

		public void cleanup(Context context) throws IOException, InterruptedException{
			while(queue.size() != 0){
				IMDB imdb = (IMDB)queue.remove();
				context.write(new Text(imdb.getString()), NullWritable.get());
			}
		}
	}

	public static class IMDBReducer2 extends Reducer<Text, NullWritable, Text, NullWritable>{
		private PriorityQueue<IMDB> queue;
		private Comparator<IMDB> comp = new IMDBComparator();
		private int topK;

		public void setup(Context context) throws IOException, InterruptedException{
			Configuration conf = context.getConfiguration();
			topK = conf.getInt("topK", -1);
			queue = new PriorityQueue<IMDB>(topK, comp);
		}

		public void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException{
			StringTokenizer tokenizer = new StringTokenizer(key.toString(), "|");
			String title = tokenizer.nextToken().trim();
			double avg_rating = Double.parseDouble(tokenizer.nextToken().trim());

			insertIMDB(queue, title,avg_rating, topK);
		}

		public void cleanup(Context context) throws IOException, InterruptedException{
			while(queue.size() != 0){
				IMDB imdb = (IMDB)queue.remove();
				context.write(new Text(imdb.title + " " + imdb.avg_rating), NullWritable.get());
			}
		}
	}

	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		String first_phase_result = "/first_phase_result";

		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if(otherArgs.length != 3){
			System.err.println("Usage : TopK <in> <out> <k>");
			System.exit(2);
		}
		int topK = Integer.parseInt(otherArgs[2]);

		Job job1 = new Job(conf, "IMDBStudent20151051_1");
		job1.setJarByClass(IMDBStudent20151051.class);
		job1.setMapperClass(IMDBMapper1.class);
		job1.setReducerClass(IMDBReducer1.class);

		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job1, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job1, new Path(first_phase_result));
		//FileOutputFormat.setOutputPath(job1, new Path(otherArgs[1]));
		//FileSystem.get(job1.getConfiguration()).delete(new Path(otherArgs[1]), true);
		FileSystem.get(job1.getConfiguration()).delete(new Path(first_phase_result), true);
	
		job1.waitForCompletion(true);

		Job job2 = new Job(conf, "IMDBStudent20151051_2");
		job2.setJarByClass(IMDBStudent20151051.class);
		job2.setMapperClass(IMDBMapper2.class);
		job2.setReducerClass(IMDBReducer2.class);

		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(NullWritable.class);

		FileInputFormat.addInputPath(job2, new Path(first_phase_result));
		FileOutputFormat.setOutputPath(job2, new Path(otherArgs[1]));
		FileSystem.get(job2.getConfiguration()).delete(new Path(otherArgs[1]), true);

		job2.waitForCompletion(true);
	}
}
