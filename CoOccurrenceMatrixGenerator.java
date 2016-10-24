import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

//find the co-occurance of two movie

public class CoOccurrenceMatrixGenerator {
    //input: value = userid\t movie1: rating, movie2: rating ...
    //output: key: movie1: movie2, value =1
    public static class MatrixGeneratorMapper extends Mapper<LongWritable, Text, Text, IntWritable> {


        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString().trim();
            String[] user_movieRating = line.split("\t");
            String[] movie_Ratings = user_movieRating[1].split(",");
            //{movie1:rating, movie2:rating..}
            if (movie_Ratings.length > 1) {
                for (int i = 0; i < movie_Ratings.length; i++) {
                    String movie1 = movie_Ratings[i].trim().split(":")[0];
                    for (int j = 0; j < movie_Ratings.length; j++) {
                        String movie2 = movie_Ratings[j].trim().split(":")[0];
                        context.write(new Text(movie1 + ":" + movie2), new IntWritable(1));
                    }
                }
            }

        }
    }
    public static class MatrixGeneratorReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            //key movie1:movie2 value = iterable<1, 1, 1>
            int sum = 0;
            while (values.iterator().hasNext()) {
                //here value is 1, but instead of  +1, we write in this format so it will always work
                sum += values.iterator().next().get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();

        //getInstanc() is the method from hadoop-mapreduce-client-core-0.23.1 jar
        Job job = Job.getInstance(conf);
        job.setMapperClass(MatrixGeneratorMapper.class);
        job.setReducerClass(MatrixGeneratorReducer.class);

        job.setJarByClass(CoOccurrenceMatrixGenerator.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        TextInputFormat.setInputPaths(job, new Path(args[0])); //input path, original dataset 

        System.exit(job.waitForCompletion(true) ? 0 : 1);


    }
}
