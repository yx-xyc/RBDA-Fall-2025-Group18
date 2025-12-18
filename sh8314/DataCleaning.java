import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DataCleaning {

    public enum DataStats { TOTAL_ROWS, VALID_ROWS, INVALID_ROWS }

    public static class CleanMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
        
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();

            if (line.startsWith("hvfhs_license_num")) return;

            context.getCounter(DataStats.TOTAL_ROWS).increment(1);

            String[] cols = line.split(",");

            // 5=Pickup, 6=Dropoff, 7=PULoc, 8=DOLoc, 10=TripTime
            if (cols.length < 11 || 
                cols[5].trim().isEmpty() || 
                cols[6].trim().isEmpty() ||
                cols[7].trim().isEmpty() || 
                cols[8].trim().isEmpty() || 
                cols[10].trim().isEmpty()) {
                context.getCounter(DataStats.INVALID_ROWS).increment(1);
                // clean data
                return;
            }

            double duration = Double.parseDouble(cols[10].trim());
            if ((long) duration <= 60) {
                 context.getCounter(DataStats.INVALID_ROWS).increment(1);
                 // clean data
                 return;
            }

            context.getCounter(DataStats.VALID_ROWS).increment(1);
            context.write(value, NullWritable.get());
        }
    }

    public static class CleanReducer extends Reducer<Text, NullWritable, Text, NullWritable> {
        public void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            context.write(key, NullWritable.get());
        }
    }

    public static void main(String[] args) throws Exception {
        Job job = Job.getInstance();
        job.setJobName("HVFHV Profiling and Cleaning");
        
        job.setJarByClass(DataCleaning.class);
        job.setMapperClass(CleanMapper.class);
        job.setNumReduceTasks(10);
        job.setReducerClass(CleanReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        if (job.waitForCompletion(true)) {
            System.out.println("\n=== REPORT ===");
            System.out.println("Total:   " + job.getCounters().findCounter(DataStats.TOTAL_ROWS).getValue());
            System.out.println("Valid:   " + job.getCounters().findCounter(DataStats.VALID_ROWS).getValue());
            System.out.println("Invalid: " + job.getCounters().findCounter(DataStats.INVALID_ROWS).getValue());
            System.out.println("===================\n");
        }
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
