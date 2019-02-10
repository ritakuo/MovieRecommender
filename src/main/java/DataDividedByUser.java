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
import java.io.IOException;

/*
DataDividedByUser class merge input data for the same user id

mapper input:
userID,movieUD,rating

mapper output:
userID,movieID:rating

reducer input:
userID,movieID:rating

Reducer output:
userID movieID:rating,movieID:rating

*/
public class DataDividedByUser {
    public static class DataDividerMapper extends
            Mapper<LongWritable, Text, IntWritable, Text>{

        public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException{
            String[] user_movie_rating = value.toString().trim().split(",");
            int userID = Integer.parseInt(user_movie_rating[0]);
            String movieID = user_movie_rating[1];
            String rating = user_movie_rating[2];
            context.write(new IntWritable(userID), new Text(movieID + ":" + rating));
        }
    }

    public static class DataDividerReducer extends
            Reducer<IntWritable, Text, IntWritable, Text>{

        @Override
        public void reduce(IntWritable key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException{
            StringBuilder sb = new StringBuilder();
            while(values.iterator().hasNext()){
                sb.append("," + values.iterator().next());
            }
            context.write(key, new Text( sb.toString().replaceFirst("," , "")));//get rid of the first comma
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Data Divider");
        job.setMapperClass(DataDividerMapper.class);
        job.setReducerClass(DataDividerReducer.class);
        job.setJarByClass(DataDividedByUser.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        TextInputFormat.setInputPaths(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
