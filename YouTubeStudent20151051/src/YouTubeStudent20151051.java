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

class YouTube{
	public String category;
	public double avg_rating;

	public YouTube(String _category, double _avg_rating){
		this.category = _category;
		this.avg_rating = _avg_rating;
	}

	public String getString(){
		return category + "|" + avg_rating;
	}
}

public class YouTubeStudent20151051{

	public static class YouTubeComparator implements Comparator<YouTube>{
		public int compare(YouTube x, YouTube y){
			if(x.avg_rating > y.avg_rating)
				return 1;
			if(x.avg_rating < y.avg_rating)
				return -1;
			return 0;
		}
	}

	public static void insertYouTube(PriorityQueue q, String category, double avg_rating, int topK){
		YouTube youtube_head = (YouTube)q.peek();

		if(q.size() < topK || youtube_head.avg_rating <= avg_rating){
			YouTube youtube = new YouTube(category, avg_rating);
			q.add(youtube);

			if(q.size() > topK)
				q.remove();
		}
	}

	public static class YouTubeMapper extends Mapper<Object, Text, Text, NullWritable>{
		private PriorityQueue<YouTube> queue;
		private Comparator<YouTube> comp = new YouTubeComparator();
		private int topK;

		protected void setup(Context context) throws IOException, InterruptedException{
			Configuration conf = context.getConfiguration();
			topK = conf.getInt("topK", -1);
			queue = new PriorityQueue<YouTube>(topK, comp);
		}

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			StringTokenizer tokenizer = new StringTokenizer(value.toString(), "|");
			String[] token_arr = new String[7];

			for(int i = 0; i < 7; i++){
				token_arr[i] = tokenizer.nextToken().trim();
			}

			String category = token_arr[3];
			double avg_rating = Double.parseDouble(token_arr[6]);

			insertYouTube(queue, category, avg_rating, topK);	
		}

		protected void cleanup(Context context) throws IOException, InterruptedException{
			while(queue.size() != 0){
				YouTube youtube = (YouTube)queue.remove();
				context.write(new Text(youtube.getString()), NullWritable.get());
			}
		}
	}

	public static class YouTubeReducer extends Reducer<Text, NullWritable, Text, NullWritable>{
		private PriorityQueue<YouTube> queue;
		private Comparator<YouTube> comp = new YouTubeComparator();
		private int topK;

		public void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException{	
			StringTokenizer tokenizer = new StringTokenizer(key.toString(), "|");
			String category = tokenizer.nextToken().trim();
			double avg_rating = Double.parseDouble(tokenizer.nextToken().trim());
			
			insertYouTube(queue, category, avg_rating, topK);	
		}

		public void setup(Context context) throws IOException, InterruptedException{
			Configuration conf = context.getConfiguration();
			topK = conf.getInt("topK", -1);
			queue = new PriorityQueue<YouTube>(topK, comp);
		}

		protected void cleanup(Context context) throws IOException, InterruptedException{
			while(queue.size() != 0){
				YouTube youtube = (YouTube)queue.remove();

				context.write(new Text(youtube.category + " " + youtube.avg_rating), NullWritable.get());
			}
		}
	}

	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if(otherArgs.length != 3){
			System.err.println("Usage : YouTubeTopK <in> <out> <topK>");
			System.exit(2);
		}
		int topK = Integer.parseInt(otherArgs[2]);
		System.out.println(topK);

		conf.setInt("topK", topK);
		Job job = new Job(conf, "YouTubeStudent20151051");
		job.setJarByClass(YouTubeStudent20151051.class);
		job.setMapperClass(YouTubeMapper.class);
		job.setReducerClass(YouTubeReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		FileSystem.get(job.getConfiguration()).delete(new Path(otherArgs[1]), true);
		
		System.exit(job.waitForCompletion(true)? 0 : 1);
	}

}
