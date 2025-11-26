import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Counter;
import java.text.SimpleDateFormat;
import java.text.ParseException;
import java.util.Date;

public class MTAFilterClean {

    public enum DataQualityCounters {
        TOTAL_RECORDS,
        VALID_RECORDS,
        FILTERED_WRONG_YEAR,
        INVALID_TIMESTAMP,
        INVALID_STATION_ID,
        INVALID_RIDERSHIP,
        NULL_PAYMENT_METHOD,
        NULL_FARE_CLASS_CATEGORY
    }

    public static class FilterCleanMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

        private SimpleDateFormat inputDateFormat = new SimpleDateFormat("MM/dd/yyyy hh:mm:ss aa");
        private SimpleDateFormat outputDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:00:00");

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            // Skip header line
            String line = value.toString();
            if (line.startsWith("transit_timestamp")) {
                return;
            }

            context.getCounter(DataQualityCounters.TOTAL_RECORDS).increment(1);

            String[] fields = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);

            // Expected columns:
            // 0:transit_timestamp, 1:transit_mode, 2:station_complex_id, 3:station_complex,
            // 4:borough, 5:payment_method, 6:fare_class_category, 7:ridership,
            // 8:transfers, 9:latitude, 10:longitude, 11:Georeference

            if (fields.length < 12) {
                context.getCounter(DataQualityCounters.INVALID_TIMESTAMP).increment(1);
                return;
            }

            // Clean quotes from fields
            for (int i = 0; i < fields.length; i++) {
                fields[i] = fields[i].trim().replaceAll("^\"|\"$", "");
            }

            // 1. Parse and validate timestamp - filter for 2024 only
            String timestamp = fields[0];
            Date parsedDate;
            try {
                parsedDate = inputDateFormat.parse(timestamp);

                // Check if year is 2024
                int year = Integer.parseInt(new SimpleDateFormat("yyyy").format(parsedDate));
                if (year != 2024) {
                    context.getCounter(DataQualityCounters.FILTERED_WRONG_YEAR).increment(1);
                    return;
                }

                // Normalize to hour level (remove minutes/seconds)
                timestamp = outputDateFormat.format(parsedDate);

            } catch (ParseException e) {
                context.getCounter(DataQualityCounters.INVALID_TIMESTAMP).increment(1);
                return;
            }

            // 2. Validate station_complex_id - drop if invalid
            String stationId = fields[2];
            if (isNullOrEmpty(stationId)) {
                context.getCounter(DataQualityCounters.INVALID_STATION_ID).increment(1);
                return;  // Drop record
            }

            // 3. Validate ridership - drop if invalid or zero
            String ridershipStr = fields[7];
            int ridership = 0;
            try {
                if (isNullOrEmpty(ridershipStr)) {
                    context.getCounter(DataQualityCounters.INVALID_RIDERSHIP).increment(1);
                    return;  // Drop record
                }
                ridership = Integer.parseInt(ridershipStr);
                if (ridership <= 0) {  // Drop records with zero or negative ridership
                    context.getCounter(DataQualityCounters.INVALID_RIDERSHIP).increment(1);
                    return;  // Drop record
                }
            } catch (NumberFormatException e) {
                context.getCounter(DataQualityCounters.INVALID_RIDERSHIP).increment(1);
                return;  // Drop record
            }

            // 4. Validate payment_method - drop if invalid
            String paymentMethod = fields[5];
            if (isNullOrEmpty(paymentMethod)) {
                context.getCounter(DataQualityCounters.NULL_PAYMENT_METHOD).increment(1);
                return;  // Drop record
            }

            // 5. Validate fare_class_category - drop if invalid
            String fareClassCategory = fields[6];
            if (isNullOrEmpty(fareClassCategory)) {
                context.getCounter(DataQualityCounters.NULL_FARE_CLASS_CATEGORY).increment(1);
                return;  // Drop record
            }

            // Build cleaned output record with 5 fields only
            // Output format: timestamp,station_complex_id,payment_method,fare_class_category,ridership
            StringBuilder output = new StringBuilder();
            output.append(timestamp).append(",");
            output.append(stationId).append(",");
            output.append(paymentMethod).append(",");
            output.append(fareClassCategory).append(",");
            output.append(ridership);

            context.getCounter(DataQualityCounters.VALID_RECORDS).increment(1);
            context.write(new Text(output.toString()), NullWritable.get());
        }

        private boolean isNullOrEmpty(String str) {
            return str == null || str.trim().isEmpty() ||
                   str.equalsIgnoreCase("null") ||
                   str.equalsIgnoreCase("none");
        }
    }

    // No reducer needed for this job, just identity reducer
    public static class FilterCleanReducer extends Reducer<Text, NullWritable, Text, NullWritable> {
        @Override
        public void reduce(Text key, Iterable<NullWritable> values, Context context)
                throws IOException, InterruptedException {
            context.write(key, NullWritable.get());
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: MTAFilterClean <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "MTA Filter and Clean 2024 Data");

        job.setJarByClass(MTAFilterClean.class);
        job.setMapperClass(FilterCleanMapper.class);
        job.setReducerClass(FilterCleanReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean success = job.waitForCompletion(true);

        // Print data quality report
        if (success) {
            System.out.println("\n=== DATA QUALITY REPORT ===");
            for (Counter counter : job.getCounters().getGroup(DataQualityCounters.class.getName())) {
                System.out.println(counter.getDisplayName() + ": " + counter.getValue());
            }
        }

        System.exit(success ? 0 : 1);
    }
}
