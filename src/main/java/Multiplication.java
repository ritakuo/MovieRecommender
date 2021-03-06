import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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
Setup:
input: movie_A:movie_B \t num_time_seem_together
output:
movieRelationMap: { movie_1 : {movie_1, movie_2, num_time_seem_together}, {movie_1, movie_2, num_time_seem_together}}
denominatorMap: { movie_1, totalOccurance}

Mapper:
input:  user_id, movie_id, rating
output user_id:movie_id dividedScore

Reducer:
Output: user_id:movie_id, sum(dividedScore)
 */

public class Multiplication {
    public static class MultiplicationMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

        Map<Integer, List<MovieRelation>> movieRelationMap = new HashMap<Integer, List<MovieRelation>>();
        Map<Integer, Integer> denominator = new HashMap<Integer, Integer>();

        protected void setup(Context context) throws IOException {
            Configuration conf = context.getConfiguration();
            String filePath = conf.get("coOccurrencePath", "job2_cooccurenceMatrix/part-r-00000");
            Path pt = new Path(filePath);
            FileSystem fs = FileSystem.get(conf);
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
            String oneLine = br.readLine();

            String[] tokens, movies;
            int movie1, movie2;

            while (oneLine != null) {
                //oneLine: movieA: movieB\t relation
                tokens = oneLine.toString().trim().split("\t");
                movies = tokens[0].split(":");
                movie1 = Integer.parseInt(movies[0]);
                movie2 = Integer.parseInt(movies[1]);
                int relation = Integer.parseInt(tokens[1]);

                MovieRelation movieRelation = new MovieRelation(movie1, movie2, relation);
                if (movieRelationMap.containsKey(movie1)) {
                    movieRelationMap.get(movie1).add(movieRelation);
                } else {
                    List<MovieRelation> list = new ArrayList<MovieRelation>();
                    list.add(movieRelation);
                    movieRelationMap.put(movie1, list);
                }
                oneLine = br.readLine();
            }
            br.close();
            /* denominator map for the total occurance of each movie
             */
            for (Map.Entry<Integer, List<MovieRelation>> entry : movieRelationMap.entrySet()) {
                int sum = 0;
                for (MovieRelation relation : entry.getValue()) {
                    sum += relation.getRelation();
                }
                denominator.put(entry.getKey(), sum);
            }
        }


        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] tokens = value.toString().trim().split(",");
            int user = Integer.parseInt(tokens[0]);
            int movie = Integer.parseInt(tokens[1]);
            double rating = Double.parseDouble(tokens[2]);

            //normalize
            for (MovieRelation relation : movieRelationMap.get(movie)) {

                //score = rating * co-occurance time between 2 movie
                double score = rating * relation.getRelation();
                //dividedScore= score/ total occurance of movie2
                double dividedScore = score / denominator.get(relation.getMovie2());
                DecimalFormat df = new DecimalFormat("#.00");
                dividedScore = Double.valueOf(df.format(dividedScore));
                context.write(new Text(user + ":" + relation.getMovie2()), new DoubleWritable(dividedScore));
            }
        }
    }
    //key: user: movie, value:dividedScore
    public static class MultiplicationReducer extends Reducer<Text, DoubleWritable, IntWritable, Text>{
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
            throws IOException, InterruptedException{

            double sum =0;
            while(values.iterator().hasNext()){
                sum += values.iterator().next().get();
            }
            String[] tokens = key.toString().split(":");
            int user =Integer.parseInt(tokens[0]);
            context.write(new IntWritable(user), new Text(tokens[1] + ":" + sum));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("coOccurrencePath", args[0]);

        Job job = Job.getInstance();
        job.setMapperClass(MultiplicationMapper.class);
        job.setReducerClass(MultiplicationReducer.class);

        job.setJarByClass(Multiplication.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        TextInputFormat.setInputPaths(job, new Path(args[1])); //output directory for Co-occurance matrix job
        TextOutputFormat.setOutputPath(job, new Path(args[2])); //output directory for this job

        job.waitForCompletion(true);
    }
}




