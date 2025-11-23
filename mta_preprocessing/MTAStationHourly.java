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
 * MapReduce Job 2: Station-Level Hourly Aggregation
 * - Aggregates ridership and transfers by station and hour
 * - Produces summary statistics for each station per hour
 * - Output: One row per station per hour
 */
public class MTAStationHourly {

    public static class AggregationMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString();
            String[] fields = line.split(",", -1);

            // Input format from Job 1:
            // timestamp,station_complex_id,station_complex,borough,
            // payment_method,fare_class_category,ridership,transfers,latitude,longitude

            if (fields.length < 10) {
                return;
            }

            String timestamp = fields[0];
            String stationId = fields[1];
            String stationName = fields[2];
            String borough = fields[3];
            String paymentMethod = fields[4];
            String fareCategory = fields[5];
            String ridershipStr = fields[6];
            String transfersStr = fields[7];
            String latitude = fields[8];
            String longitude = fields[9];

            int ridership = 0;
            int transfers = 0;

            try {
                ridership = Integer.parseInt(ridershipStr);
                transfers = Integer.parseInt(transfersStr);
            } catch (NumberFormatException e) {
                // Skip invalid records
                return;
            }

            // Create composite key: timestamp|station_id|station_name|borough|lat|lon
            String compositeKey = String.join("|",
                timestamp, stationId, stationName, borough, latitude, longitude);

            // Value contains: ridership,transfers,payment_method,fare_category
            String compositeValue = String.join("|",
                String.valueOf(ridership),
                String.valueOf(transfers),
                paymentMethod,
                fareCategory);

            context.write(new Text(compositeKey), new Text(compositeValue));
        }
    }

    public static class AggregationReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            int totalRidership = 0;
            int totalTransfers = 0;
            int recordCount = 0;

            // Track payment method distribution
            int metrocardCount = 0;
            int omnyCount = 0;

            // Track fare categories
            int fullFareCount = 0;
            int seniorDisabilityCount = 0;
            int unlimitedCount = 0;
            int studentCount = 0;
            int fairFareCount = 0;
            int otherFareCount = 0;

            for (Text value : values) {
                String[] parts = value.toString().split("\\|");
                if (parts.length < 4) continue;

                int ridership = Integer.parseInt(parts[0]);
                int transfers = Integer.parseInt(parts[1]);
                String paymentMethod = parts[2].toLowerCase();
                String fareCategory = parts[3].toLowerCase();

                totalRidership += ridership;
                totalTransfers += transfers;
                recordCount++;

                // Count payment methods
                if (paymentMethod.contains("metrocard")) {
                    metrocardCount++;
                } else if (paymentMethod.contains("omny")) {
                    omnyCount++;
                }

                // Count fare categories
                if (fareCategory.contains("full fare")) {
                    fullFareCount++;
                } else if (fareCategory.contains("senior") || fareCategory.contains("disability")) {
                    seniorDisabilityCount++;
                } else if (fareCategory.contains("unlimited")) {
                    unlimitedCount++;
                } else if (fareCategory.contains("student")) {
                    studentCount++;
                } else if (fareCategory.contains("fair fare")) {
                    fairFareCount++;
                } else {
                    otherFareCount++;
                }
            }

            // Output format:
            // timestamp,station_id,station_name,borough,latitude,longitude,
            // total_ridership,total_transfers,record_count,
            // metrocard_txns,omny_txns,
            // full_fare_txns,senior_disability_txns,unlimited_txns,student_txns,fair_fare_txns,other_txns

            String[] keyParts = key.toString().split("\\|");
            StringBuilder output = new StringBuilder();

            // Key components
            for (String part : keyParts) {
                output.append(part).append(",");
            }

            // Aggregated values
            output.append(totalRidership).append(",");
            output.append(totalTransfers).append(",");
            output.append(recordCount).append(",");
            output.append(metrocardCount).append(",");
            output.append(omnyCount).append(",");
            output.append(fullFareCount).append(",");
            output.append(seniorDisabilityCount).append(",");
            output.append(unlimitedCount).append(",");
            output.append(studentCount).append(",");
            output.append(fairFareCount).append(",");
            output.append(otherFareCount);

            context.write(null, new Text(output.toString()));
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: MTAStationHourly <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "MTA Station-Level Hourly Aggregation");

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
