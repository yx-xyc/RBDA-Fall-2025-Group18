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
 * MapReduce Job 4: Citywide Hourly Aggregation
 * - Aggregates total MTA ridership by hour across entire city
 * - Useful for analyzing overall system trends and correlation with citywide weather/crime
 * - Output: One row per hour for entire MTA system
 */
public class MTACitywideHourly {

    public static class SystemWideMapper extends Mapper<LongWritable, Text, Text, Text> {

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

            // Key is just timestamp for system-wide aggregation
            // Value: ridership,transfers,borough,payment_method,fare_category
            String compositeValue = String.join("|",
                String.valueOf(ridership),
                String.valueOf(transfers),
                borough,
                paymentMethod,
                fareCategory);

            context.write(new Text(timestamp), new Text(compositeValue));
        }
    }

    public static class SystemWideReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            int totalRidership = 0;
            int totalTransfers = 0;
            int totalStations = 0;

            // Borough breakdown
            int manhattanRidership = 0;
            int brooklynRidership = 0;
            int queensRidership = 0;
            int bronxRidership = 0;
            int statenIslandRidership = 0;

            // Payment method breakdown
            int metrocardRidership = 0;
            int omnyRidership = 0;

            // Fare category breakdown
            int fullFareRidership = 0;
            int seniorDisabilityRidership = 0;
            int unlimitedRidership = 0;
            int studentRidership = 0;
            int fairFareRidership = 0;

            for (Text value : values) {
                String[] parts = value.toString().split("\\|");
                if (parts.length < 5) continue;

                int ridership = Integer.parseInt(parts[0]);
                int transfers = Integer.parseInt(parts[1]);
                String borough = parts[2].toLowerCase();
                String paymentMethod = parts[3].toLowerCase();
                String fareCategory = parts[4].toLowerCase();

                totalRidership += ridership;
                totalTransfers += transfers;
                totalStations++;

                // Borough breakdown
                if (borough.contains("manhattan")) {
                    manhattanRidership += ridership;
                } else if (borough.contains("brooklyn")) {
                    brooklynRidership += ridership;
                } else if (borough.contains("queens")) {
                    queensRidership += ridership;
                } else if (borough.contains("bronx")) {
                    bronxRidership += ridership;
                } else if (borough.contains("staten")) {
                    statenIslandRidership += ridership;
                }

                // Payment method
                if (paymentMethod.contains("metrocard")) {
                    metrocardRidership += ridership;
                } else if (paymentMethod.contains("omny")) {
                    omnyRidership += ridership;
                }

                // Fare categories (approximation based on fare type)
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
                }
            }

            // Output format (CSV):
            // timestamp,total_ridership,total_transfers,total_stations,
            // manhattan_ridership,brooklyn_ridership,queens_ridership,bronx_ridership,staten_island_ridership,
            // metrocard_ridership,omny_ridership,
            // full_fare_ridership,senior_disability_ridership,unlimited_ridership,student_ridership,fair_fare_ridership

            StringBuilder output = new StringBuilder();
            output.append(key.toString()).append(",");
            output.append(totalRidership).append(",");
            output.append(totalTransfers).append(",");
            output.append(totalStations).append(",");
            output.append(manhattanRidership).append(",");
            output.append(brooklynRidership).append(",");
            output.append(queensRidership).append(",");
            output.append(bronxRidership).append(",");
            output.append(statenIslandRidership).append(",");
            output.append(metrocardRidership).append(",");
            output.append(omnyRidership).append(",");
            output.append(fullFareRidership).append(",");
            output.append(seniorDisabilityRidership).append(",");
            output.append(unlimitedRidership).append(",");
            output.append(studentRidership).append(",");
            output.append(fairFareRidership);

            context.write(null, new Text(output.toString()));
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: MTACitywideHourly <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "MTA Citywide Hourly Aggregation");

        job.setJarByClass(MTACitywideHourly.class);
        job.setMapperClass(SystemWideMapper.class);
        job.setReducerClass(SystemWideReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
