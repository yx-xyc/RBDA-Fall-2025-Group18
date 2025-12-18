import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WeatherAggregator {

  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("Usage: WeatherAggregator <input path> <output path>");
      System.exit(-1);
    }

    Job job = Job.getInstance();
    job.setJarByClass(WeatherAggregator.class);
    job.setJobName("HourlyWeatherAggregation");

    job.getConfiguration().set("mapreduce.output.textoutputformat.separator", ",");

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    job.setMapperClass(WeatherMapper.class);
    job.setReducerClass(WeatherReducer.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    
    job.setNumReduceTasks(4); 

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}