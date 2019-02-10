import java.io.*;
import java.text.DecimalFormat;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/*
setup:
- filter out movie that user watched before
- map movie_name to movie_id
input: 	< user,movie,rating>
output: watchedHistoryMap

mapper:
input : < user \t movie:rating>
output: < user \t movie:rating> filtered out the already watched ones

redicer:
 */
public class RecommenderListGenerator {
	public static class RecommenderListGeneratorMapper extends Mapper<LongWritable, Text, IntWritable, Text> {


		Map<Integer, List<Integer>> watchHistory = new HashMap<Integer, List<Integer>>();
		
		@Override
		protected void setup(Context context) throws IOException {
			//read movie watch history 
			Configuration conf = context.getConfiguration();
			String filePath = conf.get("watchHistory"); //userRating.txt
			Path pt = new Path(filePath);
			FileSystem fs = FileSystem.get(conf);
			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
			String line = br.readLine();
			
			while(line != null) {
				//79	30:3,84:3
				int user = Integer.parseInt(line.split("\t")[0]);
				String movieList = line.split("\t")[1];
				int movieListLen=line.split("\t")[1].split(",").length; //num of movie the person seem
				for(int i=0; i<movieListLen; i++){
					int movie= Integer.parseInt(line.split("\t")[1].split(",")[i].split(":")[0]);
					if(watchHistory.containsKey(user)) {
						watchHistory.get(user).add(movie);
					}
					else {
						List<Integer> list = new ArrayList<Integer>();
						list.add(movie);
						watchHistory.put(user, list);
					}
				}

				line = br.readLine();
			}
			br.close();
		}

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			//recommender user \t movie:rating
			String[] tokens = value.toString().split("\t");
			int user = Integer.parseInt(tokens[0]);
			int movie = Integer.parseInt(tokens[1].split(":")[0]);

			try{
				if(!watchHistory.get(user).contains(movie)) {
					context.write(new IntWritable(user), new Text(movie + ":" + tokens[1].split(":")[1]));
				}
			}catch (Exception e){
				System.out.println(e);
				System.out.println("user:" + user);
				System.out.println("movie:" + movie);
			}

		}
	}

	public static class RecommenderListGeneratorReducer extends Reducer<IntWritable, Text, IntWritable, Text> {

		Map<Integer, String> movieTitles = new HashMap<Integer, String>();
		//match movie_name to movie_id
		@Override
		protected void setup(Context context) throws IOException {
			//<movie_id, movie_title>
			Configuration conf = context.getConfiguration();
			String filePath = conf.get("movieTitles");
			Path pt = new Path(filePath);
			FileSystem fs = FileSystem.get(conf);
			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
			String line = br.readLine();
			//movieid,movie_name
			while(line != null) {
				int movie_id = Integer.parseInt(line.trim().split(",")[0]);
				movieTitles.put(movie_id, line.trim().split(",")[1]);
				line = br.readLine();
			}
			br.close();
		}

		// reduce method
		@Override
		public void reduce(IntWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			//movie_id:rating
			while(values.iterator().hasNext()) {
				String cur = values.iterator().next().toString();
				int movie_id = Integer.parseInt(cur.split(":")[0]);
				String rating = cur.split(":")[1];

				context.write(key, new Text(movieTitles.get(movie_id) + ":" + rating));
			}
		}
	}


	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		conf.set("watchHistory", args[0]);
		conf.set("movieTitles", args[1]);
		int numResults = Integer.parseInt(args[2]);


		Job job = Job.getInstance(conf);
		job.setMapperClass(RecommenderListGeneratorMapper.class);
		job.setReducerClass(RecommenderListGeneratorReducer.class);

		job.setJarByClass(RecommenderListGenerator.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);

		TextInputFormat.setInputPaths(job, new Path(args[3]));
		TextOutputFormat.setOutputPath(job, new Path(args[4]));
		String fileNameForTopNrecommendList= args[5];


		job.waitForCompletion(true);

		//post-processing, only generate top numResult result
		Map<String, List<String>> userRcommendationMap = new HashMap<String, List<String>>();
		Path pt = new Path(args[4]+"/part-r-00000");
		FileSystem fs = FileSystem.get(conf);
		BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
		String line = br.readLine();

		while(line != null) {
			if(!userRcommendationMap.containsKey(line.split("\t")[0])){
				userRcommendationMap.put(line.split("\t")[0], new ArrayList<String>());
			}
			userRcommendationMap.get(line.split("\t")[0]).add(line.split("\t")[1]);
			line = br.readLine();
		}

		Map <String, List<String>> filteredMap = new HashMap<String, List<String>>();


		for(Map.Entry<String, List<String>> entry: userRcommendationMap.entrySet()){
			List<String> valList=entry.getValue();
				Collections.sort(valList, new Comparator<String>() {
					public int compare(String s1, String s2) {
						try {
							int p1 = s1.lastIndexOf(":");
							float rating1 = Float.parseFloat(s1.substring(p1+1));
							int p2 = s2.lastIndexOf(":");
							float rating2 = Float.parseFloat(s2.substring(p2+1));
							if(rating1<rating2) return 1;
							else if(rating1 > rating2) return -1;
							else return 0;
						} catch (Exception e) {
							System.out.println("Exception handling " + s1 + " or " + s2 );
						}
						return 0;
					}
				});
			if(valList.size()>5) {
				filteredMap.put(entry.getKey(), valList.subList(0, numResults));
			}else{
				filteredMap.put(entry.getKey(), valList);
			}
		}

		//write to output file
		try{
			File outputFile = new File(fileNameForTopNrecommendList);
			FileOutputStream fos = new FileOutputStream(outputFile);
			PrintWriter pw = new PrintWriter(fos);
			for(Map.Entry<String, List<String>>m : filteredMap.entrySet()){
				pw.println(m.getKey() + " = " + m.getValue().toString());
			}
			pw.flush();
			pw.close();
			fos.close();
		}catch(Exception e) {
			System.out.println(e);
		}

	}
}
