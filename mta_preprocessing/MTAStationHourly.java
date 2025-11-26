import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * MapReduce Aggregation Job: Station-Level Hourly Aggregation
 * - Aggregates ridership by station, hour, and payment method
 * - Sums across all fare_class_category values
 * - Output: One row per station per hour per payment method
 */
public class MTAStationHourly {

    public static class AggregationMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString();
            String[] fields = line.split(",", -1);

            // Input format from cleaning job:
            // timestamp,station_complex_id,payment_method,fare_class_category,ridership

            if (fields.length < 5) {
                return;
            }

            String timestamp = fields[0];
            String stationId = fields[1];
            String paymentMethod = fields[2];
            // fields[3] is fare_class_category - we're aggregating across all fare classes
            String ridershipStr = fields[4];

            int ridership = 0;
            try {
                ridership = Integer.parseInt(ridershipStr);
            } catch (NumberFormatException e) {
                // Skip invalid records
                return;
            }

            // Composite key: timestamp|station_complex_id|payment_method
            String compositeKey = String.join("|", timestamp, stationId, paymentMethod);

            // Value is just the ridership
            context.write(new Text(compositeKey), new Text(String.valueOf(ridership)));
        }
    }

    public static class AggregationReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            int totalRidership = 0;

            // Sum all ridership values
            for (Text value : values) {
                try {
                    totalRidership += Integer.parseInt(value.toString());
                } catch (NumberFormatException e) {
                    // Skip invalid values
                    continue;
                }
            }

            // Parse key components
            String[] keyParts = key.toString().split("\\|");
            if (keyParts.length < 3) {
                return;
            }

            String timestamp = keyParts[0];
            String stationId = keyParts[1];
            String paymentMethod = keyParts[2];

            // Output format: timestamp,station_complex_id,payment_method,total_ridership
            StringBuilder output = new StringBuilder();
            output.append(timestamp).append(",");
            output.append(stationId).append(",");
            output.append(paymentMethod).append(",");
            output.append(totalRidership);

            context.write(null, new Text(output.toString()));
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: MTAStationHourly <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "MTA Station-Level Hourly Aggregation by Payment Method");

        job.setJarByClass(MTAStationHourly.class);
        job.setMapperClass(AggregationMapper.class);
        job.setReducerClass(AggregationReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
