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
 * MapReduce Job 3: Borough-Level Hourly Aggregation
 * - Aggregates ridership by borough and hour
 * - Perfect for comparing with Uber/Lyft HVFHV data (which has borough/zone)
 * - Perfect for joining with NYPD arrest data (which has borough)
 * - Output: One row per borough per hour
 */
public class MTABoroughHourly {

    public static class BoroughMapper extends Mapper<LongWritable, Text, Text, Text> {

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
            String borough = fields[3];
            String paymentMethod = fields[4];
            String fareCategory = fields[5];
            String ridershipStr = fields[6];
            String transfersStr = fields[7];

            int ridership = 0;
            int transfers = 0;

            try {
                ridership = Integer.parseInt(ridershipStr);
                transfers = Integer.parseInt(transfersStr);
            } catch (NumberFormatException e) {
                return;
            }

            // Create composite key: timestamp|borough
            String compositeKey = timestamp + "|" + borough;

            // Value: ridership,transfers,payment_method,fare_category
            String compositeValue = String.join("|",
                String.valueOf(ridership),
                String.valueOf(transfers),
                paymentMethod,
                fareCategory);

            context.write(new Text(compositeKey), new Text(compositeValue));
        }
    }

    public static class BoroughReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            int totalRidership = 0;
            int totalTransfers = 0;
            int stationCount = 0;

            // Payment method breakdown
            int metrocardRidership = 0;
            int omnyRidership = 0;

            // Fare category breakdown
            int fullFareRidership = 0;
            int seniorDisabilityRidership = 0;
            int unlimitedRidership = 0;
            int studentRidership = 0;
            int fairFareRidership = 0;
            int otherRidership = 0;

            for (Text value : values) {
                String[] parts = value.toString().split("\\|");
                if (parts.length < 4) continue;

                int ridership = Integer.parseInt(parts[0]);
                int transfers = Integer.parseInt(parts[1]);
                String paymentMethod = parts[2].toLowerCase();
                String fareCategory = parts[3].toLowerCase();

                totalRidership += ridership;
                totalTransfers += transfers;
                stationCount++;

                // Payment method breakdown
                if (paymentMethod.contains("metrocard")) {
                    metrocardRidership += ridership;
                } else if (paymentMethod.contains("omny")) {
                    omnyRidership += ridership;
                }

                // Fare category breakdown
                if (fareCategory.contains("full fare")) {
                    fullFareRidership += ridership;
                } else if (fareCategory.contains("senior") || fareCategory.contains("disability")) {
                    seniorDisabilityRidership += ridership;
                } else if (fareCategory.contains("unlimited")) {
                    unlimitedRidership += ridership;
                } else if (fareCategory.contains("student")) {
                    studentRidership += ridership;
                } else if (fareCategory.contains("fair fare")) {
                    fairFareRidership += ridership;
                } else {
                    otherRidership += ridership;
                }
            }

            // Output format (CSV):
            // timestamp,borough,total_ridership,total_transfers,station_count,
            // metrocard_ridership,omny_ridership,
            // full_fare_ridership,senior_disability_ridership,unlimited_ridership,
            // student_ridership,fair_fare_ridership,other_ridership

            String[] keyParts = key.toString().split("\\|");
            String timestamp = keyParts[0];
            String borough = keyParts[1];

            StringBuilder output = new StringBuilder();
            output.append(timestamp).append(",");
            output.append(borough).append(",");
            output.append(totalRidership).append(",");
            output.append(totalTransfers).append(",");
            output.append(stationCount).append(",");
            output.append(metrocardRidership).append(",");
            output.append(omnyRidership).append(",");
            output.append(fullFareRidership).append(",");
            output.append(seniorDisabilityRidership).append(",");
            output.append(unlimitedRidership).append(",");
            output.append(studentRidership).append(",");
            output.append(fairFareRidership).append(",");
            output.append(otherRidership);

            context.write(null, new Text(output.toString()));
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: MTABoroughHourly <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "MTA Borough-Level Hourly Aggregation");

        job.setJarByClass(MTABoroughHourly.class);
        job.setMapperClass(BoroughMapper.class);
        job.setReducerClass(BoroughReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
